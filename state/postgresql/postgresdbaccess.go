/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgresql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"

	// Blank import for the underlying Postgres driver.
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultTableName         = "state"
	defaultMetadataTableName = "dapr_metadata"
	cleanupIntervalKey       = "cleanupIntervalInSeconds"
	defaultCleanupInternal   = 3600 // In seconds = 1 hour
)

var errMissingConnectionString = errors.New("missing connection string")

// PostgresDBAccess implements dbaccess.
type PostgresDBAccess struct {
	logger          logger.Logger
	metadata        postgresMetadataStruct
	cleanupInterval *time.Duration
	db              *sql.DB
	ctx             context.Context
	cancel          context.CancelFunc
}

// newPostgresDBAccess creates a new instance of postgresAccess.
func newPostgresDBAccess(logger logger.Logger) *PostgresDBAccess {
	logger.Debug("Instantiating new Postgres state store")

	return &PostgresDBAccess{
		logger: logger,
	}
}

type postgresMetadataStruct struct {
	ConnectionString      string
	ConnectionMaxIdleTime time.Duration
	TableName             string // Could be in the format "schema.table" or just "table"
	MetadataTableName     string // Could be in the format "schema.table" or just "table"
}

// Init sets up Postgres connection and ensures that the state table exists.
func (p *PostgresDBAccess) Init(meta state.Metadata) error {
	p.logger.Debug("Initializing Postgres state store")

	p.ctx, p.cancel = context.WithCancel(context.Background())

	err := p.ParseMetadata(meta)
	if err != nil {
		p.logger.Errorf("Failed to parse metadata: %v", err)
		return err
	}

	db, err := sql.Open("pgx", p.metadata.ConnectionString)
	if err != nil {
		p.logger.Error(err)
		return err
	}

	p.db = db

	pingCtx, pingCancel := context.WithTimeout(p.ctx, 30*time.Second)
	pingErr := db.PingContext(pingCtx)
	pingCancel()
	if pingErr != nil {
		return pingErr
	}

	p.db.SetConnMaxIdleTime(p.metadata.ConnectionMaxIdleTime)
	if err != nil {
		return err
	}

	migrate := &migrations{
		Logger:            p.logger,
		Conn:              p.db,
		MetadataTableName: p.metadata.MetadataTableName,
		StateTableName:    p.metadata.TableName,
	}
	err = migrate.Perform(p.ctx)
	if err != nil {
		return err
	}

	p.ScheduleCleanupExpiredData(p.ctx)

	return nil
}

func (p *PostgresDBAccess) GetDB() *sql.DB {
	return p.db
}

func (p *PostgresDBAccess) ParseMetadata(meta state.Metadata) error {
	m := postgresMetadataStruct{
		TableName:         defaultTableName,
		MetadataTableName: defaultMetadataTableName,
	}
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}
	p.metadata = m

	if m.ConnectionString == "" {
		return errMissingConnectionString
	}

	s, ok := meta.Properties[cleanupIntervalKey]
	if ok && s != "" {
		cleanupIntervalInSec, err := strconv.ParseInt(s, 10, 0)
		if err != nil {
			return fmt.Errorf("invalid value for '%s': %s", cleanupIntervalKey, s)
		}

		// Non-positive value from meta means disable auto cleanup.
		if cleanupIntervalInSec > 0 {
			p.cleanupInterval = ptr.Of(time.Duration(cleanupIntervalInSec) * time.Second)
		}
	} else {
		p.cleanupInterval = ptr.Of(defaultCleanupInternal * time.Second)
	}

	return nil
}

// Set makes an insert or update to the database.
func (p *PostgresDBAccess) Set(ctx context.Context, req *state.SetRequest) error {
	return p.doSet(ctx, p.db, req)
}

func (p *PostgresDBAccess) doSet(parentCtx context.Context, db dbquerier, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return errors.New("missing key in set operation")
	}

	if v, ok := req.Value.(string); ok && v == "" {
		return errors.New("empty string is not allowed in set operation")
	}

	v := req.Value
	byteArray, isBinary := req.Value.([]uint8)
	if isBinary {
		v = base64.StdEncoding.EncodeToString(byteArray)
	}

	// Convert to json string
	bt, _ := stateutils.Marshal(v, json.Marshal)
	value := string(bt)

	// TTL
	var ttlSeconds int
	ttl, ttlerr := stateutils.ParseTTL(req.Metadata)
	if ttlerr != nil {
		return fmt.Errorf("error parsing TTL: %w", ttlerr)
	}
	if ttl != nil {
		ttlSeconds = *ttl
	}

	var result sql.Result

	// Sprintf is required for table name because query.DB does not substitute parameters for table names.
	// Other parameters use query.DB parameter substitution.
	var (
		query           string
		queryExpiredate string
		params          []any
	)
	if req.ETag == nil || *req.ETag == "" {
		if req.Options.Concurrency == state.FirstWrite {
			query = `INSERT INTO %[1]s
					(key, value, isbinary, expiredate)
				VALUES
					($1, $2, $3, %[2]s)`
		} else {
			query = `INSERT INTO %[1]s
					(key, value, isbinary, expiredate)
				VALUES
					($1, $2, $3, %[2]s)
				ON CONFLICT (key)
				DO UPDATE SET
					value = $2,
					isbinary = $3,
					updatedate = CURRENT_TIMESTAMP,
					expiredate = %[2]s`
		}
		params = []any{req.Key, value, isBinary}
	} else {
		// Convert req.ETag to uint32 for postgres XID compatibility
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		query = `UPDATE %[1]s
			SET
				value = $1,
				isbinary = $2,
				updatedate = CURRENT_TIMESTAMP,
				expiredate = %[2]s
			WHERE
				key = $3
				AND xmin = $4`
		params = []any{value, isBinary, req.Key, uint32(etag64)}
	}

	if ttlSeconds > 0 {
		queryExpiredate = "CURRENT_TIMESTAMP + interval '" + strconv.Itoa(ttlSeconds) + " seconds'"
	} else {
		queryExpiredate = "NULL"
	}
	result, err = db.ExecContext(parentCtx, fmt.Sprintf(query, p.metadata.TableName, queryExpiredate), params...)

	if err != nil {
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.New("no item was updated")
	}

	return nil
}

func (p *PostgresDBAccess) BulkSet(parentCtx context.Context, req []state.SetRequest) error {
	tx, err := p.db.BeginTx(parentCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if len(req) > 0 {
		for i := range req {
			err = p.doSet(parentCtx, tx, &req[i])
			if err != nil {
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (p *PostgresDBAccess) Get(parentCtx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, errors.New("missing key in get operation")
	}

	var (
		value    []byte
		isBinary bool
		etag     uint64 // Postgres uses uint32, but FormatUint requires uint64, so using uint64 directly to avoid re-allocations
	)
	query := `SELECT
			value, isbinary, xmin AS etag
		FROM %s
			WHERE
				key = $1
				AND (expiredate IS NULL OR expiredate >= CURRENT_TIMESTAMP)`
	err := p.db.
		QueryRowContext(parentCtx, fmt.Sprintf(query, p.metadata.TableName), req.Key).
		Scan(&value, &isBinary, &etag)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if err == sql.ErrNoRows {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	if isBinary {
		var (
			s    string
			data []byte
		)

		if err = json.Unmarshal(value, &s); err != nil {
			return nil, err
		}

		if data, err = base64.StdEncoding.DecodeString(s); err != nil {
			return nil, err
		}

		return &state.GetResponse{
			Data:     data,
			ETag:     ptr.Of(strconv.FormatUint(etag, 10)),
			Metadata: req.Metadata,
		}, nil
	}

	return &state.GetResponse{
		Data:     value,
		ETag:     ptr.Of(strconv.FormatUint(etag, 10)),
		Metadata: req.Metadata,
	}, nil
}

// Delete removes an item from the state store.
func (p *PostgresDBAccess) Delete(ctx context.Context, req *state.DeleteRequest) (err error) {
	return p.doDelete(ctx, p.db, req)
}

func (p *PostgresDBAccess) doDelete(parentCtx context.Context, db dbquerier, req *state.DeleteRequest) (err error) {
	if req.Key == "" {
		return errors.New("missing key in delete operation")
	}

	var result sql.Result

	if req.ETag == nil || *req.ETag == "" {
		result, err = db.ExecContext(parentCtx, "DELETE FROM state WHERE key = $1", req.Key)
	} else {
		// Convert req.ETag to uint32 for postgres XID compatibility
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		etag := uint32(etag64)

		result, err = db.ExecContext(parentCtx, "DELETE FROM state WHERE key = $1 AND xmin = $2", req.Key, etag)
	}

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 && req.ETag != nil && *req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, nil)
	}

	return nil
}

func (p *PostgresDBAccess) BulkDelete(parentCtx context.Context, req []state.DeleteRequest) error {
	tx, err := p.db.BeginTx(parentCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if len(req) > 0 {
		for i := range req {
			err = p.doDelete(parentCtx, tx, &req[i])
			if err != nil {
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (p *PostgresDBAccess) ExecuteMulti(parentCtx context.Context, request *state.TransactionalStateRequest) error {
	tx, err := p.db.BeginTx(parentCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, o := range request.Operations {
		switch o.Operation {
		case state.Upsert:
			var setReq state.SetRequest
			setReq, err = getSet(o)
			if err != nil {
				return err
			}

			err = p.doSet(parentCtx, tx, &setReq)
			if err != nil {
				return err
			}

		case state.Delete:
			var delReq state.DeleteRequest
			delReq, err = getDelete(o)
			if err != nil {
				return err
			}

			err = p.doDelete(parentCtx, tx, &delReq)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unsupported operation: %s", o.Operation)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Query executes a query against store.
func (p *PostgresDBAccess) Query(parentCtx context.Context, req *state.QueryRequest) (*state.QueryResponse, error) {
	q := &Query{
		query:     "",
		params:    []any{},
		tableName: p.metadata.TableName,
	}
	qbuilder := query.NewQueryBuilder(q)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}
	data, token, err := q.execute(parentCtx, p.logger, p.db)
	if err != nil {
		return &state.QueryResponse{}, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

func (p *PostgresDBAccess) ScheduleCleanupExpiredData(ctx context.Context) {
	if p.cleanupInterval == nil {
		return
	}

	p.logger.Infof("Schedule expired data clean up every %d seconds", int(p.cleanupInterval.Seconds()))

	go func() {
		ticker := time.NewTicker(*p.cleanupInterval)
		for {
			select {
			case <-ticker.C:
				err := p.CleanupExpired(ctx)
				if err != nil {
					p.logger.Errorf("Error removing expired data: %v", err)
				}
			case <-ctx.Done():
				p.logger.Debug("Stopped background cleanup of expired data")
				return
			}
		}
	}()
}

func (p *PostgresDBAccess) CleanupExpired(ctx context.Context) error {
	// Check if the last iteration was too recent
	// This performs an atomic operation, so allows coordination with other daprd processes too
	canContinue, err := p.UpdateLastCleanup(ctx, p.db, *p.cleanupInterval)
	if err != nil {
		// Log errors only
		p.logger.Warnf("Failed to read last cleanup time from database: %v", err)
	}
	if !canContinue {
		p.logger.Debug("Last cleanup was performed too recently")
		return nil
	}

	// Note we're not using the transaction here as we don't want this to be rolled back half-way or to lock the table unnecessarily
	// Need to use fmt.Sprintf because we can't parametrize a table name
	// Note we are not setting a timeout here as this query can take a "long" time, especially if there's no index on expiredate
	//nolint:gosec
	stmt := fmt.Sprintf(`DELETE FROM %s WHERE expiredate IS NOT NULL AND expiredate < CURRENT_TIMESTAMP`, p.metadata.TableName)
	res, err := p.db.ExecContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	cleaned, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to count affected rows: %w", err)
	}

	p.logger.Infof("Removed %d expired rows", cleaned)
	return nil
}

// UpdateLastCleanup sets the 'last-cleanup' value only if it's less than cleanupInterval.
// Returns true if the row was updated, which means that the cleanup can proceed.
func (p *PostgresDBAccess) UpdateLastCleanup(ctx context.Context, db dbquerier, cleanupInterval time.Duration) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	res, err := db.ExecContext(queryCtx,
		fmt.Sprintf(`INSERT INTO %[1]s (key, value)
			VALUES ('last-cleanup', CURRENT_TIMESTAMP)
			ON CONFLICT (key)
			DO UPDATE SET value = CURRENT_TIMESTAMP
				WHERE (EXTRACT('epoch' FROM CURRENT_TIMESTAMP - %[1]s.value::timestamp with time zone) * 1000)::bigint > $1`,
			p.metadata.MetadataTableName),
		cleanupInterval.Milliseconds()-100, // Subtract 100ms for some buffer
	)
	cancel()
	if err != nil {
		return true, fmt.Errorf("failed to execute query: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return true, fmt.Errorf("failed to count affected rows: %w", err)
	}

	return n > 0, nil
}

// Close implements io.Close.
func (p *PostgresDBAccess) Close() error {
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	if p.db != nil {
		return p.db.Close()
	}

	return nil
}

// GetCleanupInterval returns the cleanupInterval property.
// This is primarily used for tests.
func (p *PostgresDBAccess) GetCleanupInterval() *time.Duration {
	return p.cleanupInterval
}

// Returns the set requests.
func getSet(req state.TransactionalStateOperation) (state.SetRequest, error) {
	setReq, ok := req.Request.(state.SetRequest)
	if !ok {
		return setReq, errors.New("expecting set request")
	}

	if setReq.Key == "" {
		return setReq, errors.New("missing key in upsert operation")
	}

	return setReq, nil
}

// Returns the delete requests.
func getDelete(req state.TransactionalStateOperation) (state.DeleteRequest, error) {
	delReq, ok := req.Request.(state.DeleteRequest)
	if !ok {
		return delReq, errors.New("expecting delete request")
	}

	if delReq.Key == "" {
		return delReq, errors.New("missing key in upsert operation")
	}

	return delReq, nil
}

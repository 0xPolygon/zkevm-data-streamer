package db

import (
	"context"
	"fmt"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// StateDB implements the StateDB interface
type StateDB struct {
	*pgxpool.Pool
}

// NewPostgresStorage creates a new StateDB
func NewStateDB(db *pgxpool.Pool) *StateDB {
	return &StateDB{
		db,
	}
}

// NewSQLDB creates a new SQL DB
func NewSQLDB(cfg Config) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%s/%s?pool_max_conns=%d", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name, cfg.MaxConns))
	if err != nil {
		log.Errorf("Unable to parse DB config: %v\n", err)
		return nil, err
	}
	if cfg.EnableLog {
		config.ConnConfig.Logger = logger{}
	}
	conn, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Errorf("Unable to connect to database: %v\n", err)
		return nil, err
	}
	return conn, nil
}

func (db *StateDB) GetL2Blocks(ctx context.Context, limit, offset uint) ([]*L2Block, error) {
	const l2BlockSQL = "SELECT batch_num, block_num, created_at, state_root, header->>'miner' AS coinbase FROM state.l2block ORDER BY block_num ASC limit $1 offset $2"

	rows, err := db.Query(ctx, l2BlockSQL, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	l2blocks := make([]*L2Block, 0, len(rows.RawValues()))

	for rows.Next() {
		l2block, err := scanL2Block(rows)
		if err != nil {
			return nil, err
		}
		l2blocks = append(l2blocks, l2block)
	}

	return l2blocks, nil
}

func scanL2Block(row pgx.Row) (*L2Block, error) {
	l2Block := L2Block{}
	var (
		rootStr     string
		coinbaseStr string
	)
	if err := row.Scan(
		&l2Block.BatchNum,
		&l2Block.BlockNum,
		&l2Block.Timestamp,
		&rootStr,
		&coinbaseStr,
	); err != nil {
		return &l2Block, err
	}
	l2Block.StateRoot = common.HexToHash(rootStr)
	l2Block.Coinbase = common.HexToAddress(coinbaseStr)
	return &l2Block, nil
}

func (db *StateDB) GetL2Transactions(ctx context.Context, minL2Block, maxL2Block uint64) ([]*L2Transaction, error) {
	const l2TxSQL = "SELECT effective_percentage, encoded, l2_block_num from state.transaction WHERE l2_block_num between $1 and $2 ORDER BY l2_block_num ASC"

	rows, err := db.Query(ctx, l2TxSQL, minL2Block, maxL2Block)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	l2Txs := make([]*L2Transaction, 0, len(rows.RawValues()))

	for rows.Next() {
		l2Tx, err := scanL2Transaction(rows)
		if err != nil {
			return nil, err
		}
		l2Txs = append(l2Txs, l2Tx)
	}

	return l2Txs, nil
}

func scanL2Transaction(row pgx.Row) (*L2Transaction, error) {
	l2Transaction := L2Transaction{}
	var (
		encodedStr string
	)
	if err := row.Scan(
		&l2Transaction.EffectivePercentage,
		&encodedStr,
		&l2Transaction.BlockNum,
	); err != nil {
		return &l2Transaction, err
	}
	l2Transaction.Encoded = common.Hex2Bytes(encodedStr)
	l2Transaction.EncodedLength = uint32(len(l2Transaction.Encoded))
	l2Transaction.IsValid = 1
	return &l2Transaction, nil
}

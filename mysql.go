package mysql

import (
	"database/sql"
  _ "github.com/go-sql-driver/mysql"
	"time"
	"sync"
	"log"
	"errors"
)

const (
	pingWait = time.Minute * 2
)

type DB struct {
	t *sql.DB
	mutex sync.RWMutex
	stmts []*Stmt
	login string
	verbose bool
	close bool
	lastreset int64
}
type Stmt struct {
	t *sql.Stmt
	db *DB
	query string
}
type row struct {
	rows *sql.Rows
	err error
}

// Initiates the connection to MySQL
func Open(login string, verbose bool) *DB {
	db := new(DB)
	db.login = login
	db.verbose = verbose
	db.restart(0)
	go db.ping()
	return db
}

func (db *DB) restart(tim int64) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if tim > db.lastreset {
		return
	}
	if db.verbose {
		log.Println(`Closing and reopening MySQL`)
	}
	for _, s := range db.stmts {
		if len(s.query) > 0 {
			s.t.Close()
		}
	}
	if db.t != nil {
		db.t.Close()
	}
	var err error
	db.t, err = sql.Open(`mysql`, db.login)
	for err != nil {
		if db.verbose {
			log.Println(`Unable to connect to MySQL, retrying...`)
		}
		time.Sleep(time.Second)
		db.t, err = sql.Open(`mysql`, db.login)
	}
	for _, s := range db.stmts {
		if s.prepare() != nil {
			panic(errors.New(`Unable to prepare query: ` + s.query))
		}
	}
	db.lastreset = time.Now().UnixNano()
	if db.verbose {
		log.Println(`MySQL connection successful`)
	}
}

// Pings to connection every 2 minutes to keep it alive, if it appears to be down then restart the connection
func (db *DB) ping() {
	var tim int64
	for {
		time.Sleep(pingWait)
		db.mutex.RLock()
		if db.close {
			db.mutex.RUnlock()
			return
		}
		tim = time.Now().UnixNano()
		if db.t.Ping() != nil {
			if db.t.Ping() != nil { // try again
				db.mutex.RUnlock()
				db.restart(tim)
				db.mutex.RLock()
			}
		}
		db.mutex.RUnlock()
	}
}

// ============== QUERY FUNCTIONS ==============

func (s *DB) Exec(q string, args ...interface{}) (sql.Result, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	tim := time.Now().UnixNano()
	res, err := s.t.Exec(q, args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		if s.verbose {
			log.Println(`Error DB Exec:`, err)
		}
		s.mutex.RUnlock()
		s.restart(tim)
		s.mutex.RLock()
		res, err = s.t.Exec(q, args...)
	}
	return res, err
}

func (s *DB) Query(q string, args ...interface{}) (*sql.Rows, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	tim := time.Now().UnixNano()
	rows, err := s.t.Query(q, args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		if s.verbose {
			log.Println(`Error DB Query:`, err)
		}
		s.mutex.RUnlock()
		s.restart(tim)
		s.mutex.RLock()
		rows, err = s.t.Query(q, args...)
	}
	return rows, err
}

func (s *DB) QueryRow(q string, args ...interface{}) row {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	tim := time.Now().UnixNano()
	rows, err := s.t.Query(q, args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		if s.verbose {
			log.Println(`Error DB QueryRow:`, err)
		}
		s.mutex.RUnlock()
		s.restart(tim)
		s.mutex.RLock()
		rows, err = s.t.Query(q, args...)
	}
	return row{rows, err}
}

func (s *DB) Prepare(query string) (*Stmt, error) {
	stmt := &Stmt{db: s, query: query}
	err := stmt.prepare()
	s.stmts = append(s.stmts, stmt)
	return stmt, err
}

func (s *DB) MustPrepare(query string) *Stmt {
	stmt := &Stmt{db: s, query: query}
	if err := stmt.prepare(); err != nil {
		panic(err)
	}
	s.stmts = append(s.stmts, stmt)
	return stmt
}

func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	s.db.mutex.RLock()
	defer s.db.mutex.RUnlock()
	tim := time.Now().UnixNano()
	res, err := s.t.Exec(args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		if s.db.verbose {
			log.Println(`Error Stmt Exec:`, err)
		}
		s.db.mutex.RUnlock()
		s.db.restart(tim)
		s.db.mutex.RLock()
		res, err = s.t.Exec(args...)
	}
	return res, err
}

func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	s.db.mutex.RLock()
	defer s.db.mutex.RUnlock()
	tim := time.Now().UnixNano()
	rows, err := s.t.Query(args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		if s.db.verbose {
			log.Println(`Error Stmt Query:`, err)
		}
		s.db.mutex.RUnlock()
		s.db.restart(tim)
		s.db.mutex.RLock()
		rows, err = s.t.Query(args...)
	}
	return rows, err
}

func (s *Stmt) QueryRow(args ...interface{}) row {
	s.db.mutex.RLock()
	defer s.db.mutex.RUnlock()
	tim := time.Now().UnixNano()
	rows, err := s.t.Query(args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		if s.db.verbose {
			log.Println(`Error Stmt QueryRow:`, err)
		}
		s.db.mutex.RUnlock()
		s.db.restart(tim)
		s.db.mutex.RLock()
		rows, err = s.t.Query(args...)
	}
	return row{rows, err}
}

func (s *Stmt) prepare() error {
	if len(s.query) == 0 { // don't reprepare prepared query on restart if query has already been closed
		s.t = nil
		return nil
	}
	res, err := s.db.t.Prepare(s.query)
	if err != nil {
		time.Sleep(time.Second)
		res, err = s.db.t.Prepare(s.query)
		if err != nil {
			return err
		}
	}
	s.t = res
	return nil
}

// The scan function is to be used with QueryRow
func (r row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	var err error
	if !r.rows.Next() {
		if err = r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err = r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	if err = r.rows.Close(); err != nil {
		return err
	}
	return nil
}

// ============== OTHER FUNCTIONS ==============

func (s *DB) Close() error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.close = true
	return s.t.Close()
}

func (s *Stmt) Close() error {
	s.query = ``
	return s.t.Close()
}

func (s *DB) SetMaxIdleConns(n int) {
	s.t.SetMaxIdleConns(n)
}

func (s *DB) SetMaxOpenConns(n int) {
	s.t.SetMaxOpenConns(n)
}

func (s *DB) Stats() sql.DBStats {
	return s.t.Stats()
}

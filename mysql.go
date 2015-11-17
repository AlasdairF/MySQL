package mysql

import (
	"database/sql"
  _ "github.com/go-sql-driver/mysql"
	"time"
	"sync"
	"log"
)

const (
	pingWait = time.Minute * 2
	resetWait = time.Second * 10
)

type DB struct {
	t *sql.DB
	once sync.Once
	mutex sync.RWMutex
	stmts []*Stmt
	login string
	verbose bool
	close bool
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
	db.once.Do(db.restart)
	go db.ping()
	return db
}

func (db *DB) restart() {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	for _, s := range db.stmts {
		s.t.Close()
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
		stmt, err := s.prepare()
		if err != nil {
			panic(errors.New(`Unable to prepare query: ` + s.query))
		}
		s.t = stmt
	}
	if db.verbose {
		log.Println(`MySQL connection successful`)
	}
	go db.resetOnce() // Start the timer to reset the ability to restart
}

// Stops reconnection from happening within 10 seconds of a previous reconnection
func (db *DB) resetOnce() {
	time.Sleep(resetWait)
	db.once = sync.Once{}
}

// Pings to connection every 2 minutes to keep it alive, if it appears to be down then restart the connection
func (db *DB) ping() {
	for {
		time.Sleep(pingWait)
		db.mutex.RLock()
		if s.close {
			db.mutex.RUnlock()
			return
		}
		if db.t.Ping() != nil {
			if db.t.Ping() != nil { // try again
				db.mutex.RUnlock()
				db.once.Do(db.restart)
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
	res, err := s.t.Exec(q, args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		s.mutex.RUnlock()
		s.once.Do(s.restart)
		s.mutex.RLock()
		res, err = s.t.Exec(q, args...)
	}
	return res, err
}

func (s *DB) Query(q string, args ...interface{}) (*sql.Rows, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	rows, err := s.t.Query(q, args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		s.mutex.RUnlock()
		once.Do(s.restart)
		s.mutex.RLock()
		rows, err = s.t.Query(q, args...)
	}
	return rows, err
}

func (s *DB) QueryRow(q string, args ...interface{}) row {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	rows, err := s.t.Query(q, args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		s.mutex.RUnlock()
		s.once.Do(s.restart)
		s.mutex.RLock()
		rows, err = s.t.Query(q, args...)
	}
	return row{rows, err}
}

func (s *DB) Prepare(query string) (*Stmt, error) {
	stmt := &Stmt{db: s, query: query}
	err := stmt.prepare()
	return stmt, err
}

func (s *DB) Close() error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.close = true
	return s.t.Close()
}

func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	s.db.mutex.RLock()
	defer s.db.mutex.RUnlock()
	res, err := s.t.Exec(args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		s.db.mutex.RUnlock()
		s.db.once.Do(s.db.restart)
		s.db.mutex.RLock()
		res, err = s.t.Exec(args...)
	}
	return res, err
}

func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	s.db.mutex.RLock()
	defer s.db.mutex.RUnlock()
	rows, err := s.t.Query(args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		s.db.mutex.RUnlock()
		s.db.once.Do(s.db.restart)
		s.db.mutex.RLock()
		rows, err = s.t.Query(args...)
	}
	return rows, err
}

func (s *Stmt) QueryRow(args ...interface{}) row {
	s.db.mutex.RLock()
	defer s.db.mutex.RUnlock()
	rows, err := s.t.Query(args...)
	if err != nil && err != sql.ErrNoRows && err != sql.ErrTxDone {
		s.db.mutex.RUnlock()
		s.db.once.Do(s.db.restart)
		s.db.mutex.RLock()
		rows, err = s.t.Query(args...)
	}
	return row{rows, err}
}

func (s *Stmt) prepare() error {
	res, err := s.db.Prepare(s.query)
	if err != nil {
		time.Sleep(time.Second)
		res, err = s.db.Prepare(s.query)
		if err != nil {
			return err
		}
	}
	s.t = res
	return nil
}

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

func (s *Stmt) Close() error {
	return s.t.Close()
}

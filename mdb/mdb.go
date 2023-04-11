package mdb

import (
	"database/sql"
	"log"
	"time"

	"github.com/mattn/go-sqlite3"
)

type EmailEntry struct {
	Id          int64
	Email       string
	ConfirmedAt *time.Time
	OptOut      bool
}

func TryCreate(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE emails (
			id				INTEGER PRIMARY KEY,
			email			TEXT UNIQUE,
			confirmed_at	INTEGER,
			opt_out			INTEGER,
		);
	`)

	if err != nil {
		if sqlError, ok := err.(sqlite3.Error); ok {
			if sqlError.Code != 1 {
				log.Fatal(sqlError)
			} else {
				log.Fatal(err)
			}
		}
	}
}

func emailEntryFromRow(row *sql.Rows) (*EmailEntry, error) {
	var id int64
	var email string
	var confirmedAt int64
	var optOut = bool

	err := row.Scan(&id, &email, &confirmedAt, &optOut)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	t := time.Unix(confirmedAt, 0)

	return &EmailEntry{Id: id, Email: email, ConfirmedAt: &t, OptOut: optOut}, nil
}

func CreateEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`INSERT INTO
						emails(email, confirmed_at, opt_out) 
						values(?, 0, false)`, email)
	if err != nil {
		log.Panicln(err)
		return err
	}

	return nil
}

func GetEmail(db *sql.DB, email string) (*EmailEntry, error) {
	rows, err := db.Query(`select id, email, confirmed_at, opt_out
						from emails where
						email = ?`, email)
	if err != nil {
		log.Panicln(err)
		return err
	}

	defer rows.Close()

	for rows.Next() {
		return emailEntryFromRow(rows)
	}
	return nil, nil
}

func UpdateEmail(db *sql.DB, entry EmailEntry) error {
	t := entry.ConfirmedAt.Unix()
	_, err := db.Exec(`INSERT INTO
						emails(email, confirmed_at, opt_out) 
						values(?, ?, ?)
						ON CONFLICT(email) DO UPDATE SET
						confirmed_at=?
						opt_out=?`, entry.Email, t, entry.OptOut, t, entry.OptOut)

	if err != nil {
		log.Panicln(err)
		return err
	}

	return nil
}

func DeleteEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`UPDATE emails
							SET opt_out=true
							WHERE
							email = ?`, email)
	if err != nil {
		log.Panicln(err)
		return err
	}

	return nil
}

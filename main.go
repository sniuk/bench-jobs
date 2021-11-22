package main

import (
	"database/sql"
	"fmt"
	"os/exec"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorhill/cronexpr"
)

type JobCommand struct {
	Id             int
	Name           string
	Status         int
	Program        string
	Params         string
	Intervals      string
	Last_execution time.Time
	Error          string
}

func checksJobs(job JobCommand) {
	db, err := sql.Open("mysql", "root@/benchjobs?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM queue_cmd WHERE id = %d", job.Id))
	if err != nil {
		panic(err.Error())
	}

	for rows.Next() {
		var r JobCommand
		err = rows.Scan(&r.Id, &r.Name, &r.Status, &r.Program, &r.Params, &r.Intervals, &r.Last_execution, &r.Error)
		if err != nil {
			panic(err.Error())
		}

		if r.Status == 3 || r.Status == 1 {

			inserted, err := db.Query(fmt.Sprintf("INSERT INTO  `queue_cmd` ( `name`, `status`, `program`, `params`, `intervals`, `last_execution`) VALUES ('%s', %d, '%s', '%s', '%s', '%s' );", r.Name, 0, r.Program, r.Params, r.Intervals, time.Now().UTC().Format("20060102150405")))
			if err != nil {
				panic(err.Error())
			}
			inserted.Close()

		}

	}

}
func updateStateAndTimeOfJob(db *sql.DB, job JobCommand, newStare int) {
	updated, err := db.Query(fmt.Sprintf("UPDATE queue_cmd SET queue_cmd.status = %d, last_execution = %s WHERE queue_cmd.id = %d;", newStare, time.Now().UTC().Format("20060102150405"), job.Id))
	if err != nil {
		panic(err.Error())
	}
	updated.Close()
}
func updateStateAndErrorOfJob(db *sql.DB, job JobCommand, newState int, Error string) {
	updated, err := db.Query(fmt.Sprintf("UPDATE queue_cmd SET queue_cmd.status = %d, error = '%s' WHERE queue_cmd.id = %d;", newState, Error, job.Id))
	if err != nil {
		panic(err.Error())
	}
	updated.Close()
}

func updateStateOfJob(db *sql.DB, job JobCommand, newStare int) {
	updated, err := db.Query(fmt.Sprintf("UPDATE queue_cmd SET queue_cmd.status = %d WHERE queue_cmd.id = %d;", newStare, job.Id))
	if err != nil {
		panic(err.Error())
	}
	updated.Close()
}
func runCommand(wg *sync.WaitGroup, job JobCommand) {
	defer wg.Done()

	db, err := sql.Open("mysql", "root@/benchjobs")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	updateStateOfJob(db, job, 1)
	cmd := exec.Command(job.Program, job.Params)
	stdout, errcmd := cmd.Output()
	cmd.Start()
	cmd.Wait()

	cmd.Start()
	cmd.Wait()

	if errcmd != nil {

		updateStateAndErrorOfJob(db, job, 3, errcmd.Error())
		fmt.Println("Cmd Error:", errcmd.Error())
	} else {
		fmt.Println("Cmd Output:", stdout)
		updateStateAndTimeOfJob(db, job, 0)
	}

}
func main() {

	db, err := sql.Open("mysql", "root@/benchjobs?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	rows, err := db.Query("SELECT * FROM queue_cmd WHERE status = 0")
	if err != nil {
		panic(err.Error())
	}

	jobs := []JobCommand{}
	var wg sync.WaitGroup
	for rows.Next() {
		var r JobCommand
		err = rows.Scan(&r.Id, &r.Name, &r.Status, &r.Program, &r.Params, &r.Intervals, &r.Last_execution, &r.Error)
		if err != nil {
			panic(err.Error())
		}
		nextTime := cronexpr.MustParse(r.Intervals).Next(r.Last_execution)

		if time.Now().UTC().After(nextTime) {
			fmt.Println("is due command:", r.Intervals)
			fmt.Println("Last_execution:", r.Last_execution)
			fmt.Println("nextTime:", nextTime)
			fmt.Println("TimeNow:", time.Now().UTC())
			jobs = append(jobs, r)
			wg.Add(1)
			go runCommand(&wg, r)
		}

	}

	wg.Wait()
	for i, job := range jobs {
		println("Checking job :", i)
		checksJobs(job)

	}
}

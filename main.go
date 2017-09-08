package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	r "gopkg.in/gorethink/gorethink.v3"
	"gopkg.in/gorethink/gorethink.v3/types"
)

type Record struct {
	Name       string      `gorethink:"name"`
	GeoSpatial types.Point `gorethink:"area"`
}

type RecordWithDistance struct {
	Dist float64 `gorethink:"dist"`
	Doc  *Record `gorethink:"doc"`
}

const (
	DBName    = "test"
	tableName = "geospatial"
	indexName = "area"
)

var records = []Record{
	{
		Name:       "first",
		GeoSpatial: types.Point{Lon: -122.423246, Lat: 37.77929790366427},
	}, {
		Name:       "second",
		GeoSpatial: types.Point{Lon: -122.42326814543915, Lat: 37.77929963483801},
	}, {
		Name:       "third",
		GeoSpatial: types.Point{Lon: -122.4232894398445, Lat: 37.779304761831504},
	}, {
		Name:       "fourth",
		GeoSpatial: types.Point{Lon: -122.423246, Lat: 37.779478096334365},
	}, {
		Name:       "fifth",
		GeoSpatial: types.Point{Lon: -124.423246, Lat: 37.779478096334365},
	},
}

func main() {
	session, err := r.Connect(r.ConnectOpts{
		Address: "127.0.0.1",
	})
	if err != nil {
		log.Fatalln("Cannot connect: ", err)
	}

	createTable(session)
	time.Sleep(1 * time.Second)
	insertRecords(session)
	time.Sleep(1 * time.Second)
	getNearestWithDistances(session)
	time.Sleep(1 * time.Second)
	getNearest(session)
	time.Sleep(1 * time.Second)
	getNearestByName("first", session)
}

func createTable(session *r.Session) {
	fmt.Println("create table and index")
	r.DB(DBName).TableDrop(tableName).Exec(session)
	if err := r.DB(DBName).TableCreate(tableName).Exec(session); err != nil {
		log.Fatalln("Cannot create table: ", err)
	}

	if err := r.DB(DBName).Table(tableName).IndexCreate(indexName, r.IndexCreateOpts{
		Geo: true,
	}).Exec(session); err != nil {
		log.Fatalln("Cannot create index: ", err)
	}
	fmt.Println("")
}

func insertRecords(session *r.Session) {
	fmt.Println("insert records")
	for _, record := range records {
		if _, err := r.DB(DBName).Table(tableName).Insert(record).RunWrite(session); err != nil {
			log.Println("Cannot create record: ", err)
		}
	}
	fmt.Println("")
}

func getNearestWithDistances(session *r.Session) {
	fmt.Println("Get nearest records with distances")
	var rows []*RecordWithDistance
	query := r.Table(tableName).
		GetNearest(types.Point{Lon: -122.4153346282659, Lat: 37.77874812639591}, r.GetNearestOpts{Index: indexName, MaxDist: 250, MaxResults: 1024, Unit: "mi"})
	res, err := query.Run(session)
	if err != nil {
		log.Println(err)
	} else if err = res.All(&rows); err != nil {
		log.Println(err)
	}
	for k := range rows {
		printStructAsJSON(rows[k])
	}
	fmt.Println("")
}

// [
//   {
//     "dist": 0.4347245659663054,
//     "doc": {
//       "area": {
//         "$reql_type$": "GEOMETRY",
//         "coordinates": [
//           -122.423246,
//           37.77929790366427
//         ],
//         "type": "Point"
//       },
//       "id": "52d34203-585c-48d0-bf57-8adcbe2f2e9b",
//       "name": "first"
//     }
//   }
// ]
//
// Remember, this is what getNearest produces, so we need to filter out just the "doc", like this
// [
//   {
//     "area": {
//       "$reql_type$": "GEOMETRY",
//       "coordinates": [
//         -122.423246,
//         37.77929790366427
//       ],
//       "type": "Point"
//     },
//     "id": "52d34203-585c-48d0-bf57-8adcbe2f2e9b",
//     "name": "first"
//   }
// ]

func getNearest(session *r.Session) {
	fmt.Println("Get just the nearest records")
	var rows []*Record
	query := r.Table(tableName).
		GetNearest(types.Point{Lon: -122.4153346282659, Lat: 37.77874812639591}, r.GetNearestOpts{Index: indexName, MaxDist: 100, MaxResults: 1024, Unit: "mi"}).
		Do(func(doc r.Term) r.Term {
			return doc.Field("doc")
		})
	res, err := query.Run(session)
	if err != nil {
		log.Println(err)
	} else if err = res.All(&rows); err != nil {
		log.Println(err)
	}
	for k := range rows {
		printStructAsJSON(rows[k])
	}
	fmt.Println("")
}

// You can chain and filter them afterwards
func getNearestByName(name string, session *r.Session) {
	fmt.Println("Chain some additional filters")
	var rows []*Record
	query := r.Table(tableName).
		GetNearest(types.Point{Lon: -122.4153346282659, Lat: 37.77874812639591}, r.GetNearestOpts{Index: indexName, MaxDist: 100, MaxResults: 1024, Unit: "mi"}).
		Do(func(doc r.Term) r.Term {
			return doc.Field("doc")
		}).Filter(r.Row.Field("name").Eq(name))
	res, err := query.Run(session)
	if err != nil {
		log.Println(err)
	} else if err = res.All(&rows); err != nil {
		log.Println(err)
	}
	for k := range rows {
		printStructAsJSON(rows[k])
	}
	fmt.Println("")
}

func printStructAsJSON(v interface{}) {
	b, _ := json.MarshalIndent(v, "", "  ")
	log.Println(string(b))
}

package cache

import "github.com/siddontang/ledisdb/ledis"
import "github.com/siddontang/ledisdb/config"


type Cache struct {
	cfg  *config.Config
	ledis_connection *ledis.Ledis
	db *ledis.DB
}

/* New(string,bool) initializes a cache object, 
takes as input database name and a boolean parameter stating whether a compression is needed or not.
*/
func New (dbName string, Iscompression bool ) *Cache {
	cfg := config.NewConfigDefault()
	cfg.DBName = dbName
	cfg.Replication.Compression = Iscompression
	ledis_connection, err := ledis.Open(cfg)
	if err != nil {
		panic("Resource temporary unavailable, problem in opening a Connection!!!")
		
	}
	db, err := ledis_connection.Select(0)
	
	if err != nil {
		panic("Problem selecting a Database Connection!!!")
		
	}

	return &Cache{ 
		cfg ,
		ledis_connection ,
		db ,
	}


}

/* SetData(string,string,int64) stores a key-value pair in the database,
takes as input key, value and key-expiry time.
*/
func (c *Cache) SetData(key string, value string, time_to_live int64) (err error) {
		err = c.db.SetEX([]byte(key), time_to_live, []byte(value))
		return 
	}

/* GetData(string)  takes a key as input a and returns the corresponding value stored in the database
*/
func (c *Cache) GetData(key string)(value string, err error) {
	val , err := c.db.Get([]byte(key))
	value = string(val[:])	
return
}


/* DeleteData(string)  deletes a particular key from the database
*/
func (c *Cache) DeleteData(key string)(err error) {
	_, err = c.db.Del([]byte(key))
	return

	}


/* UpdateTTL(string, int64)  updates expiry time of the given key.
*/
func (c *Cache) UpdateTTL(key string,duration int64)(set_time int64, err error) {
	set_time , err = c.db.Expire([]byte(key), duration)
	return
}


/* CloseConnection() closes the connection with the database.
*/
func (c *Cache) CloseConnection(){
	c.ledis_connection.Close()
}

	

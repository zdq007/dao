package dao

import (
	. "github.com/jinzhu/gorm"
	"github.com/Sirupsen/logrus"
	"database/sql"
	"encoding/json"
	"github.com/gogap/errors"
)

type Dao struct {
	*DB
	server string
	user   string
	pwd    string
	dbname string
}

func (self *Dao) Init()  {
	logrus.Info("Init database config!")
}

//@title  产生一个数据库操作对象
//@return 返回Dao指针
func GenerateDB(args ... interface{}) (dao * Dao,err error){
	len := len(args)
	if len<1{
		return nil,errors.New("参数个数不对\n 至少传入数据库连接url( user:pwd@tcp(127.0.0.1)/dbname?charset=utf8 )")
	}
	dao = new(Dao)
	DB, err := Open("mysql",string(args[0]))
	if err != nil {
		logrus.Error("打开数据库异常：", err)
	}

	dao.DB = &DB;

	//初始连接数
	if len >1 {
		DB.DB().SetMaxIdleConns(args[1].(int))
	}
	//最大连接数
	if len >2 {
		DB.DB().SetMaxOpenConns(args[2].(int))
	}
	//显示sql
	if len >3 {
		DB.LogMode(args[2].(bool))
	}

	// Disable table name's pluralization
	DB.SingularTable(true)

	return
}

//查询一条记录 采用回调，在高调用函数的时候最好少采用回调函数的方式
func (self *Dao) QueryOneRowCallback(backfn func(row *sql.Row), sql string, args ...interface{}) {
	row := self.Raw(sql, args...).Row()
	backfn(row)
}


//查询一条记录
func (self *Dao) QueryOneRow(sql string, args ...interface{}) *sql.Row {
	row := self.Raw(sql, args...).Row()
	return row
}

//查询多条记录，在高调用函数的时候最好少采用回调函数的方式
func (self *Dao) QueryRowsCallback(backfn func(rows *sql.Rows), sql string, args ...interface{}) {
	rows, err := self.Raw(sql, args...).Rows()
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		logrus.Error("未查询到数据:", err)
	} else {
		for rows.Next() {
			backfn(rows)
		}
	}
}

//查询多条记录 （记得释放rows）
func (self *Dao) QueryRows(sql string, args ...interface{}) (*sql.Rows, error) {
	return self.Raw(sql, args...).Rows()
}


//统计记录数
func (self *Dao) QueryCount(sql string, args ...interface{}) int64 {
	var count int64
	self.Raw(sql, args...).Count(&count)
	return count
}

//查询一个字段的值
func (self *Dao) QueryOneField(sql string, args ...interface{}) (res interface{}) {
	self.Raw(sql, args...).Row().Scan(&res)
	return
}

//执行修改 删除 等操作
func (self *Dao) Execute(sql string, arges ...interface{}) int64 {
	return self.Exec(sql, arges...).RowsAffected
}

//执行新增 ，返回主键id
func (self *Dao) Save(sql string, arges ...interface{}) (int64, error) {
	res, err := self.DB.DB().Exec(sql, arges...)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

//@title  查找返回JSON数组 [{},{},{}] 因为mysql库查询返回的大部分类型都是[]BYTE  所以不能通过类型判断 只能由用户指定类型
func (self *Dao) QueryArray(sqlstr string, args ...interface{}) ([]interface{}, error) {
	rows, err := self.Raw(sqlstr, args...).Rows()
	defer rows.Close()
	if err == nil {
		columns, err := rows.Columns()
		if err == nil {
			values := make([]sql.RawBytes, len(columns)) //sql.RawBytes
			scanArgs := make([]interface{}, len(values))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			result := make([]interface{}, 0)
			for rows.Next() {
				err = rows.Scan(scanArgs...)
				if err != nil {
					panic(err.Error())
				}
				record := make(map[string]interface{})

				for i, col := range values {
					record[columns[i]] = col
					if col != nil {
						record[columns[i]] = string(col)
					}else{
						record[columns[i]] = nil
					}
				}
				result = append(result, record)
			}

			return result, nil
		}
	}
	return nil, err
}
//@title  查找返回JSON数组
//@return 字符串,异常
func (self *Dao) QueryJsonArray(sqlstr string, args ...interface{}) (string, error){
	arr,err := self.QueryArray(sqlstr,args...)
	if err!= nil{
		return  "",err
	}
	json, err := json.Marshal(arr)
	if err != nil {
		return "", err
	}
	return string(json), nil
}
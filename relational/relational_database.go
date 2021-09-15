package relational

import (
	"fmt"
	"log"
	"reflect"

	"github.com/tmdgo/environment/variables"
	"github.com/tmdgo/reflection/fields"
	"github.com/tmdgo/reflection/interfaces"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type RelationalDatabase struct {
	DB             *gorm.DB
	connectionName string
}

func (database *RelationalDatabase) Connect(name string) (err error) {
	getDsnFromEnvironment := func(connectionName string) (databaseType, dsn string) {
		errorsCount := 0

		getParameterFromEnvironment := func(name string) (value string) {
			value, err := variables.Get(fmt.Sprintf("TMD_DATABASE_%v_%v", connectionName, name))
			if err != nil {
				errorsCount++
				err = nil
			}
			return
		}

		getIntParameterFromEnvironment := func(name string) (value int64) {
			value, _ = variables.GetInt64(fmt.Sprintf("TMD_DB_%v_%v", connectionName, name))
			return
		}

		databaseType = getParameterFromEnvironment("TYPE")
		host := getParameterFromEnvironment("HOST")
		port := getIntParameterFromEnvironment("POSTGRES_PORT")
		sslMode := getParameterFromEnvironment("SSL_MODE")
		databaseName := getParameterFromEnvironment("NAME")
		user := getParameterFromEnvironment("USER")
		password := getParameterFromEnvironment("PASSWORD")

		dsn = fmt.Sprintf(
			"host=%v port=%v sslmode=%v dbname=%v user=%v password=%v",
			host,
			port,
			sslMode,
			databaseName,
			user,
			password,
		)

		return
	}

	dsn, databaseType := getDsnFromEnvironment(name)

	var dialector gorm.Dialector

	switch databaseType {
	case "postgres":
		dialector = postgres.Open(dsn)
	}

	connection, err := gorm.Open(dialector, &gorm.Config{})

	if err != nil {
		log.Panicln(err)
	}

	database.connectionName = name
	database.DB = connection
	return
}

func (database *RelationalDatabase) Create(model interface{}) (err error) {
	id, err := database.getEntityID(model)
	if err != nil {
		return
	}
	if id != 0 {
		err = fmt.Errorf(
			`relational database: it is not possible to insert a model "%v" with the pre-filled ID field`,
			interfaces.GetTypeName(model),
		)
		return
	}
	err = database.DB.Create(model).Error
	if err != nil {
		err = fmt.Errorf("relational database: %s", err)
		return
	}
	return
}

func (database *RelationalDatabase) Update(model interface{}) (err error) {
	id, err := database.getEntityID(model)
	if err != nil {
		return
	}
	if id == 0 {
		err = fmt.Errorf(
			`relational database: it is not possible to update a model "%v" with the blank ID field`,
			interfaces.GetTypeName(model),
		)
		return
	}
	err = database.DB.Save(model).Error
	if err != nil {
		err = fmt.Errorf("relational database: %s", err)
	}
	return
}

func (database *RelationalDatabase) DeleteByID(model interface{}, id uint) (err error) {
	err = database.DB.Delete(model, id).Error
	if err != nil {
		err = fmt.Errorf("relational database: %s", err)
	}
	return
}

func (database *RelationalDatabase) SelectByID(model interface{}, id uint) (err error) {
	err = database.DB.First(model, 10).Error
	if err != nil {
		err = fmt.Errorf("relational database: %s", err)
	}
	return
}

func (database *RelationalDatabase) SelectAll(models interface{}) (err error) {
	err = database.DB.Find(models).Error
	if err != nil {
		if err.Error() == "record not found" {
			err = nil
			return
		}
		err = fmt.Errorf("relational database: %s", err)
	}
	return
}

func (database *RelationalDatabase) Filter(models, modelFilter interface{}) (err error) {
	err = database.DB.Where(modelFilter).Find(models).Error
	if err != nil {
		if err.Error() == "record not found" {
			err = nil
			return
		}
		err = fmt.Errorf("relational database: %s", err)
	}
	return
}

func (database *RelationalDatabase) Transaction(function func(database RelationalDatabase) error) (err error) {
	tx := database.DB.Begin()
	txDatabase := RelationalDatabase{DB: tx, connectionName: database.connectionName}
	err = function(txDatabase)
	if err != nil {
		txDatabase.DB.Rollback()
		return
	}
	txDatabase.DB.Commit()
	return
}

func (database *RelationalDatabase) RegisterEntities(entities ...interface{}) {
	database.DB.AutoMigrate(entities...)
}

func (database *RelationalDatabase) getEntityID(entity interface{}) (id int64, err error) {
	fieldType, fieldValue, err := fields.GetTypeAndValue(entity, "ID")
	if err != nil {
		return
	}
	if reflect.TypeOf(int64(0)) != fieldType {
		err = fmt.Errorf(`the "%v" entity ID field is not of type int64`, interfaces.GetTypeName(entity))
	}
	id = fieldValue.(int64)
	return
}

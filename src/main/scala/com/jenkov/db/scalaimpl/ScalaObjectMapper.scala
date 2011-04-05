package com.jenkov.db.scalaimpl

import com.jenkov.db.impl.mapping.ObjectMapper
import java.sql.Connection
import java.lang.{String, Class}
import com.jenkov.db.itf.{PersistenceException, IPersistenceConfiguration}
import java.lang.reflect.Method
import collection.immutable.HashSet
import com.jenkov.db.ScalaClassUtil
import com.jenkov.db.itf.mapping._
import java.lang.annotation.Annotation
;
/*
 * Created by IntelliJ IDEA.
 * User: takeshita
 * Date: 11/03/16
 * Time: 1:45
 */

class ScalaObjectMapper(objectMappingFactory : IObjectMappingFactory) extends ObjectMapper(objectMappingFactory) {

  setDbNameGuesser(new ScalaDbNameGuesser())

  override def mapSettersToTable(objectClass: Class[_],
                                 om: IObjectMapping,
                                 connection: Connection,
                                 databaseName: String, table: String) = {
    val objectMapping = assureValidObjectMapping(objectClass, om, connection, databaseName, table)

    for(s <- filteredSetters(objectMapping.getObjectClass) ){
      val possibleNames = this.nameGuesser.getPossibleColumnNames(s._2)
      val dbFieldName = this.nameDeterminer.determineColumnName(possibleNames,
      objectMapping.getTableName,connection)

      if(dbFieldName != null){
        val fieldMapping = this.objectMappingFactory.createSetterMapping(s._2, dbFieldName, true);
        if(fieldMapping != null){
          fieldMapping.setColumnType(this.nameDeterminer.getColumnType(dbFieldName, objectMapping.getTableName()));
          objectMapping.addSetterMapping(fieldMapping);
        }
      }
    }

    objectMapping

  }

  def filteredSetters(clazz : Class[_]) = {
    var getters : Map[String,Method] = Map.empty
    var setters : Map[String,Method] = Map.empty
    for(m <- clazz.getMethods){
      if(isGetter(m)){
        getters +=( m.getName -> m)
      }else if(isSetter(m)){
        setters +=( m.getName.substring(0,m.getName.length - 4) -> m)
      }
    }
    setters.filter(s => {
      val name = s._1
      if(getters.contains(name)){
        val m = getters(name)
        s._2.getParameterTypes()(0) == m.getReturnType
      }else{
        false
      }
    })

  }

  override def mapGettersToTable(objectClass: Class[_],
                                 om: IObjectMapping,
                                 connection: Connection,
                                 databaseName: String, table: String) = {
    val objectMapping = assureValidObjectMapping(objectClass, om, connection, databaseName, table)
    for(m <- objectMapping.getObjectClass.getMethods if isGetter(m)){
      val possibleNames = this.nameGuesser.getPossibleColumnNames(m)
      for(i <- possibleNames.toArray){
      }
      val dbFieldName = this.nameDeterminer.determineColumnName(possibleNames,objectMapping.getTableName,null)
      if(dbFieldName != null) {
        val fieldMapping = this.objectMappingFactory.createGetterMapping(m, dbFieldName, true);
        if(fieldMapping != null){
          fieldMapping.setColumnType(this.nameDeterminer.getColumnType(dbFieldName, objectMapping.getTableName()));
          objectMapping.addGetterMapping(fieldMapping);
        }
      }
    }

    objectMapping
  }

  def isGetter(method : Method) = {
      if(method.getName == "getClass"){
        false
      }else{
        method.getReturnType.getName != "void" && method.getParameterTypes.length == 0
      }
  }
  def isSetter(method : Method) = {
    method.getName.endsWith("_$eq") && method.getReturnType.getName == "void" &&
    method.getParameterTypes.length == 1
  }

  private def assureValidObjectMapping( objectClass : Class[_], om : IObjectMapping ,
                                                    connection : Connection , databaseName : String , table : String ) : IObjectMapping = {
        val objectMapping = if(om == null){
          this.objectMappingFactory.createObjectMapping();
        }else{
          om
        }
        assureValidObjectClass          (objectMapping, objectClass);
        assureValidTableName            (objectMapping, table, connection);
        assureValidPrimaryKeyColumnName(objectMapping, databaseName, connection);

        objectMapping;
  }
  private def assureValidObjectClass(mapping : IObjectMapping ,  persistentObjectClass : Class[_]) : Unit = {
        if(persistentObjectClass == null && mapping.getObjectClass() == null){
            throw new PersistenceException("No class provided in either parameter or inside object method");
        }
        if(persistentObjectClass == null && mapping.getObjectClass() != null){
            return Unit;
        }
        if(persistentObjectClass != null && mapping.getObjectClass() == null){
            mapping.setObjectClass(persistentObjectClass);
            return Unit;
        }

        if(!persistentObjectClass.equals(mapping.getObjectClass())){
            throw new PersistenceException("The object class passed as parameter ("
                    +  persistentObjectClass.getName()
                    +  ") and the object class found in "
                    +  "the provided object method ("
                    +  mapping.getObjectClass().getName()
                    +  ") did not match. They must be the same, or only one of them should be provided. "
                    +  "You can leave out either of them (set to null) and the one present will be used.");
        }
  }

  def assureValidTableName(objectMapping : IObjectMapping, table : String, connection : Connection) : Unit = {
    if(table == null && objectMapping.getTableName != null){
      return Unit
    }
    if(table != null && objectMapping.getTableName == null){
      objectMapping.setTableName(table)
      return Unit
    }
    if(table != null && objectMapping.getTableName != null){
      if(!table.equals(objectMapping.getTableName)){
        throw new PersistenceException("Two different table names provided for object method for class " +
                        objectMapping.getObjectClass().getName() + ". Table name '" + table + "' passed as " +
                        "parameter doesn't match with table name '" + objectMapping.getTableName() +
                        "'  found in the provided object method");
      }else{
        return Unit
      }
    }

    val tableName = this.nameDeterminer.determineTableName(
    this.nameGuesser.getPossibleTableNames(objectMapping.getObjectClass),
    null,
    connection
    )

    if(tableName != null){
      objectMapping.setTableName(tableName)
      return Unit
    }else{
      throw new PersistenceException("No table found matching class " + objectMapping.getObjectClass());
    }


  }

  private def assureValidPrimaryKeyColumnName(objectMapping : IObjectMapping ,databaseName :  String , connection :  Connection) : Unit = {
        if(objectMapping.getPrimaryKey().getTable() == null){
            objectMapping.setPrimaryKey(this.primaryKeyDeterminer
                    .getPrimaryKeyMapping(objectMapping.getTableName(), databaseName, connection));
        }
  }

  override def createOrModifyMethodMappingsFromAnnotations( mapping : IObjectMapping,  configuration : IPersistenceConfiguration,
                                             connection : Connection) = {
        val clazz = mapping.getObjectClass
        val methods = mapping.getObjectClass().getMethods();

    def getAnnotation[T <: Annotation](m : Method, anno : Class[T]) : T = {
      val i = m.getAnnotation(anno).asInstanceOf[T]
      if(i != null){
        i
      }else{
        try{
          clazz.getDeclaredField(m.getName).getAnnotation(anno).asInstanceOf[T]
        }catch{
          case e : NoSuchFieldException => null.asInstanceOf[T]
        }
      }
    }

        for(method <- methods){
            if (ScalaClassUtil.isGetter(method)) {
            	val versioningAnnotation = getAnnotation(method , classOf[AVersioning])
            	if(versioningAnnotation != null){
            		var getterMapping = mapping.getGetterMapping(method);
            		// if getter mapping have been registered,remove it.
                    if(mapping.getGetterMapping(method) != null){
            			mapping.removeGetterMapping(method);
                    }
            		if(getterMapping == null){
            			getterMapping = this.objectMappingFactory.createVersioningMapping(method,null,false);
            		}else{
            			getterMapping = this.objectMappingFactory.convertToVersioning(getterMapping);
            		}
                    if(isSet(versioningAnnotation.columnName())) {
                        getterMapping.setColumnName (versioningAnnotation.columnName());
                    }
                    getterMapping.setObjectMethod(method);
                    if(isSet(versioningAnnotation.columnType())){
                        getterMapping.setColumnType   (translateColumnType(method, versioningAnnotation.columnType()));
                    }

                    mapping.addGetterMapping(getterMapping);
            	}else{
                val getterAnnotation = getAnnotation(method , classOf[AGetterMapping])
                if(getterAnnotation != null){
                    var getterMapping = mapping.getGetterMapping(method);
                    if(getterMapping == null){
                        getterMapping = this.objectMappingFactory.createGetterMapping(method, null, false);
                    }
                    if(isSet(getterAnnotation.columnName())) {
                        getterMapping.setColumnName (getterAnnotation.columnName());
                    }
                    getterMapping.setObjectMethod(method);
                    getterMapping.setAutoGenerated(getterAnnotation.databaseGenerated());
                    getterMapping.setTableMapped  (getterAnnotation.includeInWrites());
                    if(isSet(getterAnnotation.columnType())){
                        getterMapping.setColumnType   (translateColumnType(method, getterAnnotation.columnType()));
                    }
                    if(mapping.getGetterMapping(method) == null){
                        mapping.addGetterMapping(getterMapping);
                    }
                }
              }
            }
        }

        val fss = filteredSetters(mapping.getObjectClass());
        for( methodSet <- fss){
          val method = methodSet._2
            var setterAnnotation = getAnnotation (method,classOf[ASetterMapping])
            if(setterAnnotation != null){
                var setterMapping = mapping.getSetterMapping(method);
                if(setterMapping == null){
                    setterMapping = this.objectMappingFactory.createSetterMapping(method, null, false);
                }
                if(isSet(setterAnnotation.columnName())){
                    setterMapping.setColumnName(setterAnnotation.columnName());
                }
                setterMapping.setObjectMethod(method);
                if(isSet(setterAnnotation.columnType())){
                    setterMapping.setColumnType(translateColumnType(method, setterAnnotation.columnType()));
                }
                if(mapping.getSetterMapping(method) == null){
                    mapping.addSetterMapping(setterMapping);
                }
            }

        }
    }

    def translateColumnType( method : Method, columnType : String) : Int = {
        if("number".equals(columnType)){
            return java.sql.Types.NUMERIC;
        }
        if("string".equals(columnType)){
            return java.sql.Types.VARCHAR;
        }
        if("date".equals(columnType)){
            return java.sql.Types.TIMESTAMP;
        }
        if("binary".equals(columnType)){
            return java.sql.Types.BLOB;
        }
        throw new IllegalArgumentException("Annotation mapping error in method: " + method.getClass().getName()
                + "." + method.getName() + "(). 'columnType' mapping was " + columnType +
                ". The annotation 'columnType' must have one of the values: number, string, date, binary");
    }

}
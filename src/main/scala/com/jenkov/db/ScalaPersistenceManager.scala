package com.jenkov.db

import impl.mapping.ObjectMappingFactory
import itf.IDaos
import scalaimpl.ScalaObjectMapper
import javax.sql.DataSource
;
/*
 * Created by IntelliJ IDEA.
 * User: takeshita
 * Date: 11/03/16
 * Time: 16:53
 */

object ScalaPersistenceManager{
  def apply(pm : PersistenceManager) : ScalaPersistenceManager = {
    pm.getConfiguration.setObjectMapper(new ScalaObjectMapper(new ObjectMappingFactory()))
    new ScalaPersistenceManager(pm)
  }
  def apply(dataSource : DataSource) : ScalaPersistenceManager = {
    val pm = new PersistenceManager(dataSource)
    apply(pm)
  }
}

class ScalaPersistenceManager(val pm : PersistenceManager) {

  def beginConnectionScope() = {
    pm.getScopingDataSource.beginConnectionScope
  }

  def endConnectionScope(throwable : Throwable) : Unit = {
    pm.getScopingDataSource.endConnectionScope(throwable)
  }

  def endConnectionScope() : Unit = {
    pm.getScopingDataSource.endConnectionScope
  }

  def daos[T](func : (IDaos) => T) : T = {
    var daos = pm.createDaos
    try{
      val result = func(daos)
      daos.closeConnection
      daos = null
      result
    }catch{
      case e : Exception => {
        if(daos != null){
          daos.closeConnection
          daos = null
        }
        throw e
      }
    }
  }

  def transaction[T]( func : (IDaos) => T) : T = {
    var conn = pm.getScopingDataSource;
    conn.beginTransactionScope
    var daos = pm.createDaos
    try{
      val result = func(daos)
      conn.endTransactionScope
      conn = null
      daos.closeConnection
      daos = null
      result
    }catch{
      case e : Exception => {
        if(conn != null){
          conn.abortTransactionScope(e)
          conn = null
        }
        if(daos != null){
          daos.closeConnection
          daos = null
        }
        throw e
      }
    }
  }

}
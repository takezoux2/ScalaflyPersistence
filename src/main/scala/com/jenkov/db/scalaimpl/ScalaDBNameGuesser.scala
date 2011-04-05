package com.jenkov.db.scalaimpl

import com.jenkov.db.impl.mapping.DbNameGuesser
import java.lang.reflect.Method
import com.jenkov.db.ScalaClassUtil

/**
 * Created by IntelliJ IDEA.
 * User: takeshita
 * Date: 11/04/05
 * Time: 15:27
 * To change this template use File | Settings | File Templates.
 */

class ScalaDbNameGuesser extends DbNameGuesser{


  override def getPossibleColumnNames(m : Method) : java.util.Collection[_] = {

    if(ScalaClassUtil.isSetter(m)){
      val n = m.getName
      getPossibleNames(n.substring(0,n.length - 4))
    }else{
      getPossibleNames(m.getName)
    }



  }

}
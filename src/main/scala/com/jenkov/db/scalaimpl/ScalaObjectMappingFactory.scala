package com.jenkov.db.scalaimpl

import com.jenkov.db.impl.mapping.ObjectMappingFactory
import java.lang.reflect.Method
import com.jenkov.db.ScalaClassUtil

/**
 * Created by IntelliJ IDEA.
 * User: takeshita
 * Date: 11/04/05
 * Time: 15:02
 * To change this template use File | Settings | File Templates.
 */

class ScalaObjectMappingFactory extends ObjectMappingFactory{

  protected override def getMemberType(m : Method) : Class[_] = {
    if(ScalaClassUtil.isGetter(m)){
      m.getReturnType
    }else if(ScalaClassUtil.isSetter(m)){
      m.getParameterTypes()(0)
    }else{
      null
    }


  }
}
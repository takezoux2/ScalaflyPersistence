package com.jenkov.db

import java.lang.reflect.Method

/**
 * Created by IntelliJ IDEA.
 * User: takeshita
 * Date: 11/04/05
 * Time: 15:08
 * To change this template use File | Settings | File Templates.
 */

object ScalaClassUtil{

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
}
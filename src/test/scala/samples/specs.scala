package samples

import org.junit.runner.RunWith
import org.specs._
import org.specs.matcher._
import org.specs.runner.{ JUnitSuiteRunner, JUnit }
import com.jenkov.db.ScalaPersistenceManager
import com.jenkov.db.jdbc.SimpleDataSource
import scala.collection.JavaConverters._

//import org.scalacheck.Gen

/**
 * Sample specification.
 * 
 * This specification can be executed with: scala -cp <your classpath=""> ${package}.SpecsTest
 * Or using maven: mvn test
 *
 * For more information on how to write or run specifications, please visit: http://code.google.com/p/specs.
 *
 */
@RunWith(classOf[JUnitSuiteRunner])
class MySpecTest extends Specification with JUnit /*with ScalaCheck*/ {


  val sm = {
      val ds = new SimpleDataSource(
        "com.mysql.jdbc.Driver",
        "jdbc:mysql://localhost:3306/test","root","*****")
        ScalaPersistenceManager(ds)
  }

  doBeforeSpec {

    sm.daos(daos => {
      daos.getJdbcDao.update("delete from scalaobj where id > 1")
    })

  }

  "InsertOrUpdate" should{
    "insert new" in  {
      val o = new ScalaObj
      o.id = 2
      o.name = "fuga"
      o.gender = 21
      sm.daos( daos => {
        daos.getObjectDao.insert(o)
      })

      val o2 = sm.daos(daos => {
        daos.getObjectDao.readByPrimaryKey(classOf[ScalaObj],2L)
      })
      o2.id must_== o.id
      o2.name must_== o.name
      o2.gender must_== o.gender
    }
    "update " in {
      val o = new ScalaObj
      o.id = 2
      o.name = "wahoo"
      o.gender = 24
      sm.daos( daos => {
        daos.getObjectDao.update(o)
      })

      val o2 = sm.daos(daos => {
        daos.getObjectDao.readByPrimaryKey(classOf[ScalaObj],2L)
      })
      o2.id must_== o.id
      o2.name must_== o.name
      o2.gender must_== o.gender
    }
  }

  "delete" should {
    "delete" in{

      val o = new ScalaObj
      o.id = 3
      o.name = "aaa"
      o.gender = 24
      sm.daos( daos => {
        daos.getObjectDao.insert(o)
      })
      sm.daos( daos => {
        daos.getObjectDao.delete(o) must_== 1
      })
    }
  }


  "read" should {
    "read by primary key" in {
      val o = sm.daos( daos => {
        val d = daos.getObjectDao.readByPrimaryKey(classOf[ScalaObj],1L)

        println("id:%s name:%s gender:%s".format(d.id,d.name,d.gender))

        d.id must_==(1L)
        d.name must_==("hoge")
        d.gender must_==(3)
      })

      //0
    }
    "read partial" in {
      val o : ScalaObj = sm.daos(daos => {
        daos.getObjectDao.read(classOf[ScalaObj],"select name from scalaobj where id = 1")
      })
      o.id must_==(0L)
      o.name must_==("hoge")
      o.gender must_==(0)
    }
    "read list" in {
      val list : List[ScalaObj] = sm.daos(daos => {
        daos.getObjectDao.readList(classOf[ScalaObj],"select * from scalaobj").asScala.toList
      })

      list.size must_== 2

    }

  }

  
  "A List" should {
    "have a size method returning the number of elements in the list" in {
      List(1, 2, 3).size must_== 3
    }
    // add more examples here
    // ...
  }

}

object MySpecMain {
  def main(args: Array[String]) {
    new MySpecTest().main(args)
  }
}

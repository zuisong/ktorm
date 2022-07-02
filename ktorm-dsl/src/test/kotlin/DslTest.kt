import org.ktorm.database.*
import org.ktorm.dsl.*
import org.ktorm.schema.*
import kotlin.test.*

class DslTest {

    @Test
    fun test() {

        val dsl = Dsl()

        dsl
            .insert(Customers) {
                set(it.id, 1)
            }
            .let { (sql, args) ->
                println(sql)
                println(args)
                println()
            }


        val e = Employees.aliased("e")
        val d = Departments.aliased("d")
        dsl.from(e)
            .innerJoin(d, on = (d.id eq e.departmentId) and (d.location eq "六楼"))
            .select(e.columns)
            .whereWithConditions {
                it += (d.name eq "技术部")
            }
            .executeQuery()
            .let { (sql, args) ->
                println(sql)
                println(args)
                println()
                assertEquals(args.size, 2)
                assertEquals(args[0].value, "六楼")
                assertEquals(args[0].sqlType, VarcharSqlType)

                assertEquals(args[1].value, "技术部")
                assertEquals(args[1].sqlType, VarcharSqlType)
            }


    }
}

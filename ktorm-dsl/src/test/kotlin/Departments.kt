import org.ktorm.database.*
import org.ktorm.dsl.*
import org.ktorm.schema.*
import java.time.*


data class Department(
    val id: Int,
    var name: String,
    var location: String,
    var mixedCase: String?,
)

@Suppress("DEPRECATION")
data class Employee(
    var id: Int,
    var name: String,
    var job: String,
    var manager: Employee?,
    var hireDate: LocalDate,
    var salary: Long,
    var departmentId: String,
)

data class Customer(
    var id: Int,
    var name: String,
    var email: String,
    var phoneNumber: String,
)


open class Departments(alias: String?) : BaseTable<Department>("t_department", alias) {
    companion object : Departments(null)

    override fun aliased(alias: String) = Departments(alias)

    val id = int("id").primaryKey()
    val name = varchar("name")
    val location = varchar("location")
    val mixedCase = varchar("mixedCase")
}

open class Employees(alias: String?) : BaseTable<Employee>("t_employee", alias) {
    companion object : Employees(null)

    override fun aliased(alias: String) = Employees(alias)

    val id = int("id").primaryKey()
    val name = varchar("name")
    val job = varchar("job")
    val managerId = int("manager_id")
    val hireDate = date("hire_date")
    val salary = long("salary")
    val departmentId = int("department_id")
}

open class Customers(alias: String?) : BaseTable<Customer>("t_customer", alias, schema = "company") {
    companion object : Customers(null)

    override fun aliased(alias: String) = Customers(alias)

    val id = int("id").primaryKey()
    val name = varchar("name")
    val email = varchar("email")
    val phoneNumber = varchar("phone_number")
}


/*
 * Copyright 2018-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ktorm.dsl

import org.ktorm.database.*
import org.ktorm.expression.*
import org.ktorm.schema.*
import java.sql.*

/**
 * [Query] is an abstraction of query operations and the core class of Ktorm's query DSL.
 *
 * The constructor of this class accepts two parameters: [dsl] is the database instance that this query
 * is running on; [expression] is the abstract representation of the executing SQL statement. Usually, we don't
 * use the constructor to create [Query] objects but use the `database.from(..).select(..)` syntax instead.
 *
 * [Query] provides a built-in [iterator], so we can iterate the results by a for-each loop:
 *
 * ```kotlin
 * for (row in database.from(Employees).select()) {
 *     println(row[Employees.name])
 * }
 * ```
 *
 * Moreover, there are many extension functions that can help us easily process the query results, such as
 * [Query.map], [Query.flatMap], [Query.associate], [Query.fold], etc. With the help of these functions, we can
 * obtain rows from a query just like it's a common Kotlin collection.
 *
 * Query objects are immutable. Query DSL functions are provided as its extension functions normally. We can
 * chaining call these functions to modify them and create new query objects. Here is a simple example:
 *
 * ```kotlin
 * val query = database
 *     .from(Employees)
 *     .select(Employees.salary)
 *     .where { (Employees.departmentId eq 1) and (Employees.name like "%vince%") }
 * ```
 *
 * Easy to know that the query obtains the salary of an employee named vince in department 1. The generated
 * SQL is obviously:
 *
 * ```sql
 * select t_employee.salary as t_employee_salary
 * from t_employee
 * where (t_employee.department_id = ?) and (t_employee.name like ?)
 * ```
 *
 * More usages can be found in the documentations of those DSL functions.
 *
 * @property dsl the [Dsl] instance that this query is running on.
 * @property expression the underlying SQL expression of this query object.
 */
public class Query(public val dsl: Dsl, public val expression: QueryExpression) {

    /**
     * The executable SQL string of this query.
     *
     * Useful when we want to ensure if the generated SQL is expected while debugging.
     */
    public val sql: String by lazy(LazyThreadSafetyMode.NONE) {
        dsl.formatExpression(expression, beautifySql = true).first
    }

    /**
     * Return a copy of this [Query] with the [expression] modified.
     */
    public fun withExpression(expression: QueryExpression): Query {
        return Query(dsl, expression)
    }
}

/**
 * Create a query object, selecting the specific columns or expressions from this [QuerySource].
 *
 * Note that the specific columns can be empty, that means `select *` in SQL.
 *
 * @since 2.7
 */
public fun QuerySource.select(columns: Collection<ColumnDeclaring<*>>): Query {
    val declarations = columns.map { it.asDeclaringExpression() }
    return Query(dsl, SelectExpression(columns = declarations, from = expression))
}

/**
 * Create a query object, selecting the specific columns or expressions from this [QuerySource].
 *
 * Note that the specific columns can be empty, that means `select *` in SQL.
 *
 * @since 2.7
 */
public fun QuerySource.select(vararg columns: ColumnDeclaring<*>): Query {
    return select(columns.asList())
}

/**
 * Create a query object, selecting the specific columns or expressions from this [QuerySource] distinctly.
 *
 * Note that the specific columns can be empty, that means `select distinct *` in SQL.
 *
 * @since 2.7
 */
public fun QuerySource.selectDistinct(columns: Collection<ColumnDeclaring<*>>): Query {
    val declarations = columns.map { it.asDeclaringExpression() }
    return Query(dsl, SelectExpression(columns = declarations, from = expression, isDistinct = true))
}

/**
 * Create a query object, selecting the specific columns or expressions from this [QuerySource] distinctly.
 *
 * Note that the specific columns can be empty, that means `select distinct *` in SQL.
 *
 * @since 2.7
 */
public fun QuerySource.selectDistinct(vararg columns: ColumnDeclaring<*>): Query {
    return selectDistinct(columns.asList())
}

/**
 * Wrap this expression as a [ColumnDeclaringExpression].
 */
internal fun <T : Any> ColumnDeclaring<T>.asDeclaringExpression(): ColumnDeclaringExpression<T> {
    return when (this) {
        is ColumnDeclaringExpression -> this
        is Column -> this.aliased(label)
        else -> this.aliased(null)
    }
}

/**
 * Specify the `where` clause of this query using the given condition expression.
 */
public fun Query.where(condition: ColumnDeclaring<Boolean>): Query {
    return this.withExpression(
        when (expression) {
            is SelectExpression -> expression.copy(where = condition.asExpression())
            is UnionExpression -> throw IllegalStateException("Where clause is not supported in a union expression.")
        }
    )
}

/**
 * Specify the `where` clause of this query using the expression returned by the given callback function.
 */
public inline fun Query.where(condition: () -> ColumnDeclaring<Boolean>): Query {
    return where(condition())
}

/**
 * Create a mutable list, then add filter conditions to the list in the given callback function, finally combine
 * them with the [and] operator and set the combined condition as the `where` clause of this query.
 *
 * Note that if we don't add any conditions to the list, the `where` clause would not be set.
 */
public inline fun Query.whereWithConditions(block: (MutableList<ColumnDeclaring<Boolean>>) -> Unit): Query {
    var conditions: List<ColumnDeclaring<Boolean>> = ArrayList<ColumnDeclaring<Boolean>>().apply(block)

    if (conditions.isEmpty()) {
        return this
    } else {
        while (conditions.size > 1) {
            conditions = conditions.chunked(2) { chunk -> if (chunk.size == 2) chunk[0] and chunk[1] else chunk[0] }
        }

        return this.where { conditions[0] }
    }
}


/**
 * The [ResultSet] object of this query, lazy initialized after first access, obtained from the database by
 * executing the generated SQL.
 *
 * Note that the return type of this property is not a normal [ResultSet], but a [QueryRowSet] instead. That's
 * a special implementation provided by Ktorm, different from normal result sets, it is available offline and
 * overrides the indexed access operator. More details can be found in the documentation of [QueryRowSet].
 */
public fun Query.executeQuery(): Pair<String, List<ArgumentExpression<*>>> {
   return dsl.formatExpression(expression)
}


/**
 * Create a mutable list, then add filter conditions to the list in the given callback function, finally combine
 * them with the [or] operator and set the combined condition as the `where` clause of this query.
 *
 * Note that if we don't add any conditions to the list, the `where` clause would not be set.
 */
public inline fun Query.whereWithOrConditions(block: (MutableList<ColumnDeclaring<Boolean>>) -> Unit): Query {
    var conditions: List<ColumnDeclaring<Boolean>> = ArrayList<ColumnDeclaring<Boolean>>().apply(block)

    if (conditions.isEmpty()) {
        return this
    } else {
        while (conditions.size > 1) {
            conditions = conditions.chunked(2) { chunk -> if (chunk.size == 2) chunk[0] or chunk[1] else chunk[0] }
        }

        return this.where { conditions[0] }
    }
}

/**
 * Combine this iterable of boolean expressions with the [and] operator.
 *
 * If the iterable is empty, the param [ifEmpty] will be returned.
 */
public fun Iterable<ColumnDeclaring<Boolean>>.combineConditions(ifEmpty: Boolean = true): ColumnDeclaring<Boolean> {
    return this.reduceOrNull { a, b -> a and b } ?: ArgumentExpression(ifEmpty, BooleanSqlType)
}

/**
 * Specify the `group by` clause of this query using the given columns or expressions.
 */
public fun Query.groupBy(columns: Collection<ColumnDeclaring<*>>): Query {
    return this.withExpression(
        when (expression) {
            is SelectExpression -> expression.copy(groupBy = columns.map { it.asExpression() })
            is UnionExpression -> throw IllegalStateException("Group by clause is not supported in a union expression.")
        }
    )
}

/**
 * Specify the `group by` clause of this query using the given columns or expressions.
 */
public fun Query.groupBy(vararg columns: ColumnDeclaring<*>): Query {
    return groupBy(columns.asList())
}

/**
 * Specify the `having` clause of this query using the given condition expression.
 */
public fun Query.having(condition: ColumnDeclaring<Boolean>): Query {
    return this.withExpression(
        when (expression) {
            is SelectExpression -> expression.copy(having = condition.asExpression())
            is UnionExpression -> throw IllegalStateException("Having clause is not supported in a union expression.")
        }
    )
}

/**
 * Specify the `having` clause of this query using the expression returned by the given callback function.
 */
public inline fun Query.having(condition: () -> ColumnDeclaring<Boolean>): Query {
    return having(condition())
}

/**
 * Specify the `order by` clause of this query using the given order-by expressions.
 */
public fun Query.orderBy(orders: Collection<OrderByExpression>): Query {
    return this.withExpression(
        when (expression) {
            is SelectExpression -> expression.copy(orderBy = orders.toList())
            is UnionExpression -> {
                val replacer = OrderByReplacer(expression)
                expression.copy(orderBy = orders.map { replacer.visit(it) as OrderByExpression })
            }
        }
    )
}

/**
 * Specify the `order by` clause of this query using the given order-by expressions.
 */
public fun Query.orderBy(vararg orders: OrderByExpression): Query {
    return orderBy(orders.asList())
}

private class OrderByReplacer(query: UnionExpression) : SqlExpressionVisitor() {
    val declaringColumns = query.findDeclaringColumns()

    override fun visitOrderBy(expr: OrderByExpression): OrderByExpression {
        val declaring = declaringColumns.find { it.declaredName != null && it.expression == expr.expression }

        if (declaring == null) {
            throw IllegalArgumentException("Could not find the ordering column in the union expression, column: $expr")
        } else {
            return OrderByExpression(
                expression = ColumnExpression(
                    table = null,
                    name = declaring.declaredName!!,
                    sqlType = declaring.expression.sqlType
                ),
                orderType = expr.orderType
            )
        }
    }
}

internal tailrec fun QueryExpression.findDeclaringColumns(): List<ColumnDeclaringExpression<*>> {
    return when (this) {
        is SelectExpression -> columns
        is UnionExpression -> left.findDeclaringColumns()
    }
}

/**
 * Order this column or expression in ascending order.
 */
public fun ColumnDeclaring<*>.asc(): OrderByExpression {
    return OrderByExpression(asExpression(), OrderType.ASCENDING)
}

/**
 * Order this column or expression in descending order, corresponding to the `desc` keyword in SQL.
 */
public fun ColumnDeclaring<*>.desc(): OrderByExpression {
    return OrderByExpression(asExpression(), OrderType.DESCENDING)
}

/**
 * Specify the pagination offset parameter of this query.
 *
 * This function requires a dialect enabled, different SQLs will be generated with different dialects.
 *
 * Note that if the number isn't positive then it will be ignored.
 */
public fun Query.offset(n: Int): Query {
    return limit(offset = n, limit = null)
}

/**
 * Specify the pagination limit parameter of this query.
 *
 * This function requires a dialect enabled, different SQLs will be generated with different dialects.
 *
 * Note that if the number isn't positive then it will be ignored.
 */
public fun Query.limit(n: Int): Query {
    return limit(offset = null, limit = n)
}

/**
 * Specify the pagination parameters of this query.
 *
 * This function requires a dialect enabled, different SQLs will be generated with different dialects. For example,
 * `limit ?, ?` by MySQL, `limit m offset n` by PostgreSQL.
 *
 * Note that if the numbers aren't positive, they will be ignored.
 */
public fun Query.limit(offset: Int?, limit: Int?): Query {
    return this.withExpression(
        when (expression) {
            is SelectExpression -> expression.copy(
                offset = offset?.takeIf { it > 0 } ?: expression.offset,
                limit = limit?.takeIf { it > 0 } ?: expression.limit
            )

            is UnionExpression -> expression.copy(
                offset = offset?.takeIf { it > 0 } ?: expression.offset,
                limit = limit?.takeIf { it > 0 } ?: expression.limit
            )
        }
    )
}

/**
 * Union this query with the given one, corresponding to the `union` keyword in SQL.
 */
public fun Query.union(right: Query): Query {
    return this.withExpression(UnionExpression(left = expression, right = right.expression, isUnionAll = false))
}

/**
 * Union this query with the given one, corresponding to the `union all` keyword in SQL.
 */
public fun Query.unionAll(right: Query): Query {
    return this.withExpression(UnionExpression(left = expression, right = right.expression, isUnionAll = true))
}

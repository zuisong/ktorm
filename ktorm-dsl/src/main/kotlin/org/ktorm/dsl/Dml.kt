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
 * Construct an update expression in the given closure, then execute it and return the effected row count.
 *
 * Usage:
 *
 * ```kotlin
 * database.update(Employees) {
 *     set(it.job, "engineer")
 *     set(it.managerId, null)
 *     set(it.salary, 100)
 *     where {
 *         it.id eq 2
 *     }
 * }
 * ```
 *
 * @since 2.7
 * @param table the table to be updated.
 * @param block the DSL block, an extension function of [UpdateStatementBuilder], used to construct the expression.
 * @return the effected row count.
 */
public fun <T : BaseTable<*>> Dsl.update(
    table: T,
    block: UpdateStatementBuilder.(T) -> Unit,
): Pair<String, List<ArgumentExpression<*>>> {
    val builder = UpdateStatementBuilder().apply { block(table) }

    val expression = AliasRemover.visit(
        UpdateExpression(table.asExpression(), builder.assignments, builder.where?.asExpression())
    )

    return formatExpression(expression)
}

/**
 * Construct update expressions in the given closure, then batch execute them and return the effected
 * row counts for each expression.
 *
 * Note that this function is implemented based on [Statement.addBatch] and [Statement.executeBatch],
 * and any item in a batch operation must have the same structure, otherwise an exception will be thrown.
 *
 * Usage:
 *
 * ```kotlin
 * database.batchUpdate(Departments) {
 *     for (i in 1..2) {
 *         item {
 *             set(it.location, "Hong Kong")
 *             where {
 *                 it.id eq i
 *             }
 *         }
 *     }
 * }
 * ```
 *
 * @since 2.7
 * @param table the table to be updated.
 * @param block the DSL block, extension function of [BatchUpdateStatementBuilder], used to construct the expressions.
 * @return the effected row counts for each sub-operation.
 */
public fun <T : BaseTable<*>> Dsl.batchUpdate(
    table: T,
    block: BatchUpdateStatementBuilder<T>.() -> Unit,
): List<Pair<String, List<ArgumentExpression<*>>>> {
    val builder = BatchUpdateStatementBuilder(table).apply(block)
    val expressions = builder.expressions.map { AliasRemover.visit(it) }

    if (expressions.isEmpty()) {
        return emptyList()
    } else {
        return expressions.map { formatExpression(it) }
    }
}

/**
 * Construct an insert expression in the given closure, then execute it and return the effected row count.
 *
 * Usage:
 *
 * ```kotlin
 * database.insert(Employees) {
 *     set(it.name, "jerry")
 *     set(it.job, "trainee")
 *     set(it.managerId, 1)
 *     set(it.hireDate, LocalDate.now())
 *     set(it.salary, 50)
 *     set(it.departmentId, 1)
 * }
 * ```
 *
 * @since 2.7
 * @param table the table to be inserted.
 * @param block the DSL block, an extension function of [AssignmentsBuilder], used to construct the expression.
 * @return the effected row count.
 */
public fun <T : BaseTable<*>> Dsl.insert(
    table: T,
    block: AssignmentsBuilder.(T) -> Unit,
): Pair<String, List<ArgumentExpression<*>>> {
    val builder = AssignmentsBuilder().apply { block(table) }
    val expression = AliasRemover.visit(InsertExpression(table.asExpression(), builder.assignments))
    return formatExpression(expression)
}

/**
 * Construct insert expressions in the given closure, then batch execute them and return the effected
 * row counts for each expression.
 *
 * Note that this function is implemented based on [Statement.addBatch] and [Statement.executeBatch],
 * and any item in a batch operation must have the same structure, otherwise an exception will be thrown.
 *
 * Usage:
 *
 * ```kotlin
 * database.batchInsert(Employees) {
 *     item {
 *         set(it.name, "jerry")
 *         set(it.job, "trainee")
 *         set(it.managerId, 1)
 *         set(it.hireDate, LocalDate.now())
 *         set(it.salary, 50)
 *         set(it.departmentId, 1)
 *     }
 *     item {
 *         set(it.name, "linda")
 *         set(it.job, "assistant")
 *         set(it.managerId, 3)
 *         set(it.hireDate, LocalDate.now())
 *         set(it.salary, 100)
 *         set(it.departmentId, 2)
 *     }
 * }
 * ```
 *
 * @since 2.7
 * @param table the table to be inserted.
 * @param block the DSL block, extension function of [BatchInsertStatementBuilder], used to construct the expressions.
 * @return the effected row counts for each sub-operation.
 */
public fun <T : BaseTable<*>> Dsl.batchInsert(
    table: T,
    block: BatchInsertStatementBuilder<T>.() -> Unit,
): List<Pair<String, List<ArgumentExpression<*>>>> {
    val builder = BatchInsertStatementBuilder(table).apply(block)
    val expressions = builder.expressions.map { AliasRemover.visit(it) }

    if (expressions.isEmpty()) {
        return emptyList()
    } else {
        return expressions.map { formatExpression(it) }
    }
}

/**
 * Insert the current [Query]'s results into the given table, useful when transfer data from a table to another table.
 */
public fun Query.insertTo(table: BaseTable<*>, vararg columns: Column<*>): Pair<String, List<ArgumentExpression<*>>> {
    val expression = InsertFromQueryExpression(
        table = table.asExpression(),
        columns = columns.map { it.asExpression() },
        query = this.expression
    )

    return dsl.formatExpression(expression)
}

/**
 * Delete the records in the [table] that matches the given [predicate].
 *
 * @since 2.7
 */
public fun <T : BaseTable<*>> Dsl.delete(
    table: T,
    predicate: (T) -> ColumnDeclaring<Boolean>,
): Pair<String, List<ArgumentExpression<*>>> {
    val expression = AliasRemover.visit(
        DeleteExpression(table.asExpression(), predicate(table).asExpression())
    )
    return formatExpression(expression)
}

/**
 * Delete all the records in the table.
 *
 * @since 2.7
 */
public fun Dsl.deleteAll(table: BaseTable<*>): Pair<String, List<ArgumentExpression<*>>> {
    val expression = AliasRemover.visit(
        DeleteExpression(table.asExpression(), where = null)
    )
    return formatExpression(expression)
}

/**
 * Marker annotation for Ktorm DSL builder classes.
 */
@DslMarker
public annotation class KtormDsl

/**
 * Base class of DSL builders, provide basic functions used to build assignments for insert or update DSL.
 */
@KtormDsl
public open class AssignmentsBuilder {
    @Suppress("VariableNaming")
    protected val _assignments: ArrayList<ColumnAssignmentExpression<*>> = ArrayList()

    /**
     * A getter that returns the readonly view of the built assignments list.
     */
    internal val assignments: List<ColumnAssignmentExpression<*>> get() = _assignments

    /**
     * Assign the specific column's value to another column or an expression's result.
     *
     * @since 3.1.0
     */
    public fun <C : Any> set(column: Column<C>, expr: ColumnDeclaring<C>) {
        _assignments += ColumnAssignmentExpression(column.asExpression(), expr.asExpression())
    }

    /**
     * Assign the specific column to a value.
     *
     * @since 3.1.0
     */
    public fun <C : Any> set(column: Column<C>, value: C?) {
        _assignments += ColumnAssignmentExpression(column.asExpression(), column.wrapArgument(value))
    }
}

/**
 * DSL builder for update statements.
 */
@KtormDsl
public class UpdateStatementBuilder : AssignmentsBuilder() {
    internal var where: ColumnDeclaring<Boolean>? = null

    /**
     * Specify the where clause for this update statement.
     */
    public fun where(block: () -> ColumnDeclaring<Boolean>) {
        this.where = block()
    }
}

/**
 * DSL builder for batch update statements.
 */
@KtormDsl
public class BatchUpdateStatementBuilder<T : BaseTable<*>>(internal val table: T) {
    internal val expressions = ArrayList<SqlExpression>()

    /**
     * Add an update statement to the current batch operation.
     */
    public fun item(block: UpdateStatementBuilder.(T) -> Unit) {
        val builder = UpdateStatementBuilder()
        builder.block(table)

        expressions += UpdateExpression(table.asExpression(), builder.assignments, builder.where?.asExpression())
    }
}

/**
 * DSL builder for batch insert statements.
 */
@KtormDsl
public class BatchInsertStatementBuilder<T : BaseTable<*>>(internal val table: T) {
    internal val expressions = ArrayList<SqlExpression>()

    /**
     * Add an insert statement to the current batch operation.
     */
    public fun item(block: AssignmentsBuilder.(T) -> Unit) {
        val builder = AssignmentsBuilder()
        builder.block(table)

        expressions += InsertExpression(table.asExpression(), builder.assignments)
    }
}

/**
 * [SqlExpressionVisitor] implementation used to removed table aliases, used by Ktorm internal.
 */
internal object AliasRemover : SqlExpressionVisitor() {

    override fun visitTable(expr: TableExpression): TableExpression {
        if (expr.tableAlias == null) {
            return expr
        } else {
            return expr.copy(tableAlias = null)
        }
    }

    override fun <T : Any> visitColumn(expr: ColumnExpression<T>): ColumnExpression<T> {
        if (expr.table == null) {
            return expr
        } else {
            return expr.copy(table = null)
        }
    }
}

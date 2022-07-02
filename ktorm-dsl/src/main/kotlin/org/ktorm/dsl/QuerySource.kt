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

/**
 * Represents a query source, used in the `from` clause of a query.
 *
 * @since 2.7
 * @property dsl the [Dsl] instance that the query is running on.
 * @property sourceTable the origin source table.
 * @property expression the underlying SQL expression.
 */
public data class QuerySource(
    val dsl: Dsl,
    val sourceTable: BaseTable<*>,
    val expression: QuerySourceExpression
)

/**
 * Wrap the specific table as a [QuerySource].
 *
 * @since 2.7
 */
public fun Dsl.from(table: BaseTable<*>): QuerySource {
    return QuerySource(this, table, table.asExpression())
}

/**
 * Join the right table and return a new [QuerySource], translated to `cross join` in SQL.
 */
public fun QuerySource.crossJoin(right: BaseTable<*>, on: ColumnDeclaring<Boolean>? = null): QuerySource {
    return this.copy(
        expression = JoinExpression(
            type = JoinType.CROSS_JOIN,
            left = expression,
            right = right.asExpression(),
            condition = on?.asExpression()
        )
    )
}

/**
 * Join the right table and return a new [QuerySource], translated to `inner join` in SQL.
 */
public fun QuerySource.innerJoin(right: BaseTable<*>, on: ColumnDeclaring<Boolean>? = null): QuerySource {
    return this.copy(
        expression = JoinExpression(
            type = JoinType.INNER_JOIN,
            left = expression,
            right = right.asExpression(),
            condition = on?.asExpression()
        )
    )
}

/**
 * Join the right table and return a new [QuerySource], translated to `left join` in SQL.
 */
public fun QuerySource.leftJoin(right: BaseTable<*>, on: ColumnDeclaring<Boolean>? = null): QuerySource {
    return this.copy(
        expression = JoinExpression(
            type = JoinType.LEFT_JOIN,
            left = expression,
            right = right.asExpression(),
            condition = on?.asExpression()
        )
    )
}

/**
 * Join the right table and return a new [QuerySource], translated to `right join` in SQL.
 */
public fun QuerySource.rightJoin(right: BaseTable<*>, on: ColumnDeclaring<Boolean>? = null): QuerySource {
    return this.copy(
        expression = JoinExpression(
            type = JoinType.RIGHT_JOIN,
            left = expression,
            right = right.asExpression(),
            condition = on?.asExpression()
        )
    )
}


private infix fun ColumnDeclaring<*>.eq(column: ColumnDeclaring<*>): BinaryExpression<Boolean> {
    return BinaryExpression(BinaryExpressionType.EQUAL, asExpression(), column.asExpression(), BooleanSqlType)
}

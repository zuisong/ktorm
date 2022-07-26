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

package org.ktorm.support.mysql

import org.ktorm.database.Database
import org.ktorm.dsl.AssignmentsBuilder
import org.ktorm.dsl.KtormDsl
import org.ktorm.expression.ColumnAssignmentExpression
import org.ktorm.expression.UpdateExpression
import org.ktorm.schema.BaseTable
import org.ktorm.schema.ColumnDeclaring


public fun <T : BaseTable<*>> Database.updateWithAlias(
    table: T, block: MysqlUpdateStatementBuilder.(T) -> Unit,
): Int {
    val builder = MysqlUpdateStatementBuilder().apply { block(table) }
    val expression = UpdateExpression(table.asExpression(), builder.assignments, builder.where?.asExpression())
    return executeUpdate(expression)
}

/**
 * DSL builder for update statements with table alias.
 */
@KtormDsl
public class MysqlUpdateStatementBuilder : AssignmentsBuilder() {
    internal var where: ColumnDeclaring<Boolean>? = null

    /**
     * Specify the where clause for this update statement.
     */
    public fun where(block: () -> ColumnDeclaring<Boolean>) {
        this.where = block()
    }

    internal val assignments: List<ColumnAssignmentExpression<*>> get() = _assignments
}


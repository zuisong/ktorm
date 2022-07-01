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

package org.ktorm.database

import org.ktorm.dsl.*
import org.ktorm.expression.*
import org.ktorm.logging.*
import java.sql.*
import java.util.*
import javax.sql.*

/**
 * The entry class of Ktorm, represents a physical database, used to manage connections and transactions.
 *
 * ### Connect with a URL
 *
 * The simplest way to create a database instance, using a JDBC URL:
 *
 * ```kotlin
 * val database = Database.connect("jdbc:mysql://localhost:3306/ktorm", user = "root", password = "123")
 * ```
 *
 * Easy to know what we do in the [connect] function. Just like any JDBC boilerplate code, Ktorm loads the MySQL
 * database driver first, then calls [DriverManager.getConnection] with your URL to obtain a connection.
 *
 * Of course, Ktorm doesn't call [DriverManager.getConnection] in the beginning. Instead, we obtain connections
 * only when it's really needed (such as executing a SQL), then close them after they are not useful anymore.
 * Therefore, database objects created by this way won't reuse any connections, creating connections frequently
 * can lead to huge performance costs. It's highly recommended to use connection pools in your production environment.
 *
 * ### Connect with a Pool
 *
 * Ktorm doesn't limit you, you can use any connection pool you like, such as DBCP, C3P0 or Druid. The [connect]
 * function provides an overloaded version which accepts a [DataSource] parameter, you just need to create a
 * [DataSource] object and call that function with it:
 *
 * ```kotlin
 * val dataSource = SingleConnectionDataSource() // Any DataSource implementation is OK.
 * val database = Database.connect(dataSource)
 * ```
 *
 * Now, Ktorm will obtain connections from the [DataSource] when necessary, then return them to the pool after they
 * are not useful. This avoids the performance costs of frequent connection creation.
 *
 * Connection pools are applicative and effective in most cases, we highly recommend you manage your connections
 * in this way.
 *
 * ### Use SQL DSL & Sequence APIs
 *
 * Now that we've connected to the database, we can perform many operations on it. Ktorm's APIs are mainly divided
 * into two parts, they are SQL DSL and sequence APIs.
 *
 * Here, we use SQL DSL to obtains the names of all engineers in department 1:
 *
 * ```kotlin
 * database
 *     .from(Employees)
 *     .select(Employees.name)
 *     .where { (Employees.departmentId eq 1) and (Employees.job eq "engineer") }
 *     .forEach { row ->
 *         println(row[Employees.name])
 *     }
 * ```
 *
 * Equivalent code using sequence APIs:
 *
 * ```kotlin
 * database
 *     .sequenceOf(Employees)
 *     .filter { it.departmentId eq 1 }
 *     .filter { it.job eq "engineer" }
 *     .mapColumns { it.name }
 *     .forEach { name ->
 *         println(name)
 *     }
 * ```
 * More details about SQL DSL, see [Query], about sequence APIs, see [EntitySequence].
 */
public class Dsl(

    /**
     * The dialect, auto detects an implementation by default using JDK [ServiceLoader] facility.
     */
    public val dialect: SqlDialect = detectDialectImplementation(),

    /**
     * The logger used to output logs, auto detects an implementation by default.
     */
    public val logger: Logger = detectLoggerImplementation(),

    /**
     * Function used to translate SQL exceptions so as to rethrow them to users.
     */
    public val exceptionTranslator: ((SQLException) -> Throwable)? = null,

    /**
     * Whether we need to always quote SQL identifiers in the generated SQLs.
     *
     * @since 3.1.0
     */
    public val alwaysQuoteIdentifiers: Boolean = false,

    /**
     * Whether we need to output the generated SQLs in upper case.
     *
     * `true` for upper case, `false` for lower case, `null` for default (the database preferred style).
     *
     * @since 3.1.0
     */
    public val generateSqlInUpperCase: Boolean? = null,
    /**
     * A set of all of this database's SQL keywords (including SQL:2003 keywords), all in uppercase.
     */
    public val keywords: Set<String> = ANSI_SQL_2003_KEYWORDS,

    /**
     * The string used to quote SQL identifiers, returns an empty string if identifier quoting is not supported.
     */
    public val identifierQuoteString: String = "",

    /**
     * All the "extra" characters that can be used in unquoted identifier names (those beyond a-z, A-Z, 0-9 and _).
     */
    public val extraNameCharacters: String = "",

    /**
     * Whether this database treats mixed case unquoted SQL identifiers as case sensitive and as a result
     * stores them in mixed case.
     *
     * @since 3.1.0
     */
    public val supportsMixedCaseIdentifiers: Boolean = false,

    /**
     * Whether this database treats mixed case unquoted SQL identifiers as case insensitive and
     * stores them in mixed case.
     *
     * @since 3.1.0
     */
    public val storesMixedCaseIdentifiers: Boolean = false,

    /**
     * Whether this database treats mixed case unquoted SQL identifiers as case insensitive and
     * stores them in upper case.
     *
     * @since 3.1.0
     */
    public val storesUpperCaseIdentifiers: Boolean = false,

    /**
     * Whether this database treats mixed case unquoted SQL identifiers as case insensitive and
     * stores them in lower case.
     *
     * @since 3.1.0
     */
    public val storesLowerCaseIdentifiers: Boolean = false,

    /**
     * Whether this database treats mixed case quoted SQL identifiers as case sensitive and as a result
     * stores them in mixed case.
     *
     * @since 3.1.0
     */
    public val supportsMixedCaseQuotedIdentifiers: Boolean = false,

    /**
     * Whether this database treats mixed case quoted SQL identifiers as case insensitive and
     * stores them in mixed case.
     *
     * @since 3.1.0
     */
    public val storesMixedCaseQuotedIdentifiers: Boolean = false,

    /**
     * Whether this database treats mixed case quoted SQL identifiers as case insensitive and
     * stores them in upper case.
     *
     * @since 3.1.0
     */
    public val storesUpperCaseQuotedIdentifiers: Boolean = false,

    /**
     * Whether this database treats mixed case quoted SQL identifiers as case insensitive and
     * stores them in lower case.
     *
     * @since 3.1.0
     */
    public val storesLowerCaseQuotedIdentifiers: Boolean = false,

    /**
     * The maximum number of characters this database allows for a column name. Zero means that there is no limit
     * or the limit is not known.
     *
     * @since 3.1.0
     */
    public val maxColumnNameLength: Int = 0,
) {

    /**
     * Format the specific [SqlExpression] to an executable SQL string with execution arguments.
     *
     * @param expression the expression to be formatted.
     * @param beautifySql output beautiful SQL strings with line-wrapping and indentation, default to `false`.
     * @param indentSize the indent size, default to 2.
     * @return a [Pair] combines the SQL string and its execution arguments.
     */
    public fun formatExpression(
        expression: SqlExpression,
        beautifySql: Boolean = false,
        indentSize: Int = 2,
    ): Pair<String, List<ArgumentExpression<*>>> {
        val formatter = dialect.createSqlFormatter(this, beautifySql, indentSize)
        formatter.visit(expression)
        return Pair(formatter.sql, formatter.parameters)
    }
}

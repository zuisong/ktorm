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

import org.ktorm.expression.*
import java.util.*

/**
 * Representation of a SQL dialect.
 *
 * It's known that there is a uniform standard for SQL language, but beyond the standard, many databases still have
 * their special features. The interface provides an extension mechanism for Ktorm and its extension modules to support
 * those dialect-specific SQL features.
 *
 * Implementations of this interface are recommended to be published as separated modules independent of ktorm-core.
 *
 * To enable a dialect, applications should add the dialect module to the classpath first, then configure the `dialect`
 * parameter to the dialect implementation while creating database instances via [Dsl.connect] functions.
 *
 * Since version 2.4, Ktorm's dialect modules start following the convention of JDK [ServiceLoader] SPI, so we don't
 * need to specify the `dialect` parameter explicitly anymore while creating [Dsl] instances. Ktorm auto detects
 * one for us from the classpath. We just need to insure the dialect module exists in the dependencies.
 */
public interface SqlDialect {

    /**
     * Create a [SqlFormatter] instance, formatting SQL expressions as strings with their execution arguments.
     *
     * @param dsl the current database instance executing the formatted SQL.
     * @param beautifySql if we should output beautiful SQL strings with line-wrapping and indentation.
     * @param indentSize the indent size.
     * @return a [SqlFormatter] object, generally typed of subclasses to support dialect-specific sql expressions.
     */
    public fun createSqlFormatter(dsl: Dsl, beautifySql: Boolean, indentSize: Int): SqlFormatter {
        return object : SqlFormatter(dsl, beautifySql, indentSize) {
            override fun writePagination(expr: QueryExpression) {
                throw DialectFeatureNotSupportedException("Pagination is not supported in Standard SQL.")
            }
        }
    }
}

/**
 * Thrown to indicate that a feature is not supported by the current dialect.
 *
 * @param message the detail message, which is saved for later retrieval by [Throwable.message].
 * @param cause the cause, which is saved for later retrieval by [Throwable.cause].
 */
public class DialectFeatureNotSupportedException(
    message: String? = null,
    cause: Throwable? = null
) : UnsupportedOperationException(message, cause) {

    private companion object {
        private const val serialVersionUID = 1L
    }
}

/**
 * Auto detect a dialect implementation.
 */
public fun detectDialectImplementation(): SqlDialect {
    val dialects = ServiceLoader.load(SqlDialect::class.java).toList()
    return when (dialects.size) {
        0 -> object : SqlDialect { }
        1 -> dialects[0]
        else -> error(
            "More than one dialect implementations found in the classpath, please choose one manually: $dialects"
        )
    }
}

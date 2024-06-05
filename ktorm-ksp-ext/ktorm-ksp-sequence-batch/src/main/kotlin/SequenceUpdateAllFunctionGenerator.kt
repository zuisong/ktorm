/*
 * Copyright 2018-2021 the original author or authors.
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
package org.ktorm.ksp.compiler.generator;

import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.symbol.ClassKind
import com.squareup.kotlinpoet.*
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.ksp.*
import org.ktorm.entity.EntitySequence
import org.ktorm.ksp.spi.ExtCodeGenerator
import org.ktorm.ksp.spi.TableMetadata


public class SequenceUpdateAllFunctionGenerator : ExtCodeGenerator {

    private val batchUpdate = MemberName("org.ktorm.dsl", "batchUpdate", true)

    @KotlinPoetKspPreview
    override fun generateFunctions(table: TableMetadata, environment: SymbolProcessorEnvironment): List<FunSpec> {

        if (table.entityClass.classKind == ClassKind.INTERFACE) {
            return emptyList()
        }

        val primaryKeyColumns = table.columns.filter { it.isPrimaryKey }

        if (primaryKeyColumns.isEmpty()) {
            return emptyList()
        }

        val entityClass = table.entityClass.toClassName()
        val tableClass = ClassName(table.entityClass.packageName.asString(), table.tableClassName)

        val addAll = FunSpec.builder("updateAll").addKdoc(
            """
                Batch update based on entity primary key
                @return the effected row counts for each sub-operll ation.
            """.trimIndent()
        ).receiver(EntitySequence::class.asClassName().parameterizedBy(entityClass, tableClass)).addParameters(
            listOf(
                ParameterSpec.builder("entities", Iterable::class.asClassName().parameterizedBy(entityClass)).build(), ParameterSpec.builder("isDynamic", typeNameOf<Boolean>()).defaultValue("false").build()
            )
        ).returns(IntArray::class.asClassName()).addCode(checkForDml()).addCode(buildCodeBlock {
            withControlFlow("if (!entities.iterator().hasNext())") {
                addStatement("return intArrayOf()")
            }
            withControlFlow("return·this.database.%M(%T)", arrayOf(batchUpdate, ClassName.bestGuess(table.tableClassName))) {
                withControlFlow("for (entity in entities)") {
                    withControlFlow("item") {
                        for (column in table.columns) {
                            if (!column.isPrimaryKey) {
                                addStatement(
                                    "set(%T.%N,·entity.%N)",
                                    ClassName.bestGuess(table.tableClassName),
                                    column.columnPropertyName,
                                    column.entityProperty.simpleName.asString()
                                )
                            }
                        }
                        withControlFlow("where") {


                            for ((i, column) in primaryKeyColumns.withIndex()) {

                                add(
                                    if (column.entityProperty.type.resolve().isMarkedNullable) {
                                        "(%T.%N·%M·entity.%N!!)"
                                    } else {
                                        "(%T.%N·%M·entity.%N)"
                                    },
                                    ClassName.bestGuess(table.tableClassName),
                                    column.columnPropertyName,
                                    MemberName("org.ktorm.dsl", "eq", true),
                                    column.entityProperty.simpleName.asString()
                                )

                                if (i < primaryKeyColumns.lastIndex) {
                                    add("·%M·", MemberName("org.ktorm.dsl", "and", true))
                                }
                            }

                            add("\n")


                        }


                    }
                }
            }
        }).build()

        return listOf(addAll)
    }
}







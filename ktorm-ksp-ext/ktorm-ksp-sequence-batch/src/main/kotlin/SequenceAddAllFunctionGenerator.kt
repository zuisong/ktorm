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
import com.google.devtools.ksp.symbol.Nullability
import com.squareup.kotlinpoet.*
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.ksp.*
import org.ktorm.entity.EntitySequence
import org.ktorm.ksp.spi.ExtCodeGenerator
import org.ktorm.ksp.spi.TableMetadata

public inline fun CodeBlock.Builder.withControlFlow(
    controlFlow: String,
    args: Array<Any?> = emptyArray(),
    block: CodeBlock.Builder.() -> Unit
): CodeBlock.Builder = apply {
    beginControlFlow(controlFlow, *args)
    block(this)
    endControlFlow()
}

public class SequenceAddAllFunctionGenerator : ExtCodeGenerator {

    private val batchInsert = MemberName("org.ktorm.dsl", "batchInsert", true)

    @KotlinPoetKspPreview
    override fun generateFunctions(table: TableMetadata, environment: SymbolProcessorEnvironment): List<FunSpec> {

        if (table.entityClass.classKind == ClassKind.INTERFACE) {
            return emptyList()
        }

        val entityClass = table.entityClass.toClassName()
        val tableClass = ClassName(table.entityClass.packageName.asString(), table.tableClassName)

        val addAll = FunSpec.builder("addAll")
            .addKdoc(
                """
                Batch insert entities into the database, this method will not get the auto-incrementing primary key
                @return the effected row counts for each sub-operation.
            """.trimIndent()
            )
            .receiver(EntitySequence::class.asClassName().parameterizedBy(entityClass, tableClass))
            .addParameters(
                listOf(
                    ParameterSpec.builder("entities", Iterable::class.asClassName().parameterizedBy(entityClass)).build(),
                    ParameterSpec.builder("isDynamic", typeNameOf<Boolean>())
                        .defaultValue("false")
                        .build()
                )
            )
            .returns(IntArray::class.asClassName())
            .addCode(checkForDml())
            .addCode(buildCodeBlock {
                withControlFlow("if (!entities.iterator().hasNext())") {
                    addStatement("return intArrayOf()")
                }
                withControlFlow("return·this.database.%M(%T)", arrayOf(batchInsert, ClassName.bestGuess(table.tableClassName))) {
                    withControlFlow("for (entity in entities)") {
                        withControlFlow("item") {
                            for (column in table.columns) {
                                val isNullable: Boolean = column.entityProperty.type.resolve().nullability != Nullability.NOT_NULL

                                if (isNullable) {
                                    beginControlFlow("if ( !isDynamic || ·entity.%N != null )", column.entityProperty.simpleName.asString())
                                }
                                addStatement(
                                    "set(%T.%N,·entity.%N)",
                                    ClassName.bestGuess(table.tableClassName),
                                    column.columnPropertyName,
                                    column.entityProperty.simpleName.asString()
                                )
                                if (isNullable) {
                                    endControlFlow()
                                }

                            }
                        }
                    }
                }
            })
            .build()

        return listOf(addAll)
    }
}

internal fun checkForDml(): CodeBlock {

    return CodeBlock.of(
        """
                val isModified = expression.where != null
                    || expression.groupBy.isNotEmpty()
                    || expression.having != null
                    || expression.isDistinct
                    || expression.orderBy.isNotEmpty()
                    || expression.offset != null
                    || expression.limit != null
                if (isModified) {
                    val msg = "" +
                        "Entity manipulation functions are not supported by this sequence object. " +
                        "Please call on the origin sequence returned from database.sequenceOf(table)"
                    throw UnsupportedOperationException(msg)
                }
                
                
            """.trimIndent()
    )
}





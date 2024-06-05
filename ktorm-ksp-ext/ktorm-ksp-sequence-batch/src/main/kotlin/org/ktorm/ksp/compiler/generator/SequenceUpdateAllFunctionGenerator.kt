package org.ktorm.ksp.compiler.generator

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
            withControlFlow("return·this.database.%M(%T)", arrayOf(batchUpdate, tableClass)) {
                withControlFlow("for (entity in entities)") {
                    withControlFlow("item") {
                        for (column in table.columns) {
                            if (!column.isPrimaryKey) {
                                val isNullable =    column.entityProperty.type.resolve().isMarkedNullable
                                if (isNullable) {
                                    beginControlFlow("if ( !isDynamic || ·entity.%N != null )", column.entityProperty.simpleName.asString())
                                }
                                addStatement(
                                    "set(%T.%N,·entity.%N)",
                                    tableClass,
                                    column.columnPropertyName,
                                    column.entityProperty.simpleName.asString()
                                )
                                if (isNullable) {
                                    endControlFlow()
                                }
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
                                    tableClass,
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









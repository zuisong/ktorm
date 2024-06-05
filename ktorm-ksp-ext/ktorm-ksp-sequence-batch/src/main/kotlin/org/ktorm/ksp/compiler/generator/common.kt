package org.ktorm.ksp.compiler.generator

import com.squareup.kotlinpoet.CodeBlock


public inline fun CodeBlock.Builder.withControlFlow(
    controlFlow: String,
    args: Array<Any?> = emptyArray(),
    block: CodeBlock.Builder.() -> Unit
): CodeBlock.Builder = apply {
    beginControlFlow(controlFlow, *args)
    block(this)
    endControlFlow()
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

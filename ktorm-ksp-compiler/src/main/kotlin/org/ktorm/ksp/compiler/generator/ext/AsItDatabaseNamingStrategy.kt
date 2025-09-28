package org.ktorm.ksp.compiler.generator.ext

import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import org.ktorm.ksp.spi.TableMetadata


public object AsItDatabaseNamingStrategy : org.ktorm.ksp.spi.DatabaseNamingStrategy {

    override fun getColumnName(c: KSClassDeclaration, prop: KSPropertyDeclaration): String = prop.simpleName.getShortName()

    override fun getRefColumnName(c: KSClassDeclaration, prop: KSPropertyDeclaration, ref: TableMetadata): String = prop.simpleName.getShortName()

    override fun getTableName(c: KSClassDeclaration): String = c.simpleName.getShortName()
}


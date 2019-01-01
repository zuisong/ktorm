package me.liuwj.ktorm.database

import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy
import org.springframework.transaction.support.TransactionCallback
import org.springframework.transaction.support.TransactionTemplate
import java.lang.reflect.UndeclaredThrowableException
import java.sql.Connection
import javax.sql.DataSource

/**
 * Created by vince on Sep 24, 2018.
 */
class SpringManagedTransactionManager(
    val dataSource: DataSource,
    val transactionTemplate: TransactionTemplate
) : TransactionManager {

    val dataSourceProxy = dataSource as? TransactionAwareDataSourceProxy ?: TransactionAwareDataSourceProxy(dataSource)

    override val defaultIsolation get() = TransactionIsolation.valueOf(transactionTemplate.isolationLevel)

    override val currentTransaction: Transaction? = null

    override fun newTransaction(isolation: TransactionIsolation): Nothing {
        val msg = "Can not create a transaction manually, please use Spring's @Transactional annotation instead."
        throw UnsupportedOperationException(msg)
    }

    override fun newConnection(): Connection {
        return dataSourceProxy.connection
    }

    override fun <T> transactional(func: () -> T): T {
        val callback = TransactionCallback {
            try {
                func()
            } catch (e: RuntimeException) {
                throw e
            } catch (e: Error) {
                throw e
            } catch (e: Throwable) {
                throw UndeclaredThrowableException(e)
            }
        }

        return transactionTemplate.execute(callback) as T
    }
}
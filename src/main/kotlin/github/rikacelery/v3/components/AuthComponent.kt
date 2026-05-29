package github.rikacelery.v3.components

import github.rikacelery.v3.core.Actor
import github.rikacelery.v3.core.EventBus
import github.rikacelery.v3.data.User
import github.rikacelery.v3.events.*
import github.rikacelery.v3.storage.MongoStore
import kotlinx.coroutines.CoroutineScope
import java.io.File
import java.util.concurrent.ConcurrentHashMap

sealed interface AuthMsg
data class HandleAuthQuery(val env: CommandEnvelope) : AuthMsg
data class LoadUsers(val users: List<User>) : AuthMsg
data class OnAuthEvent(val event: Any) : AuthMsg

class AuthComponent(
    private val usersPath: String,
    eventBus: EventBus,
    parentScope: CoroutineScope,
    private val mongo: MongoStore = MongoStore(null)
) : Actor<AuthMsg>("AuthComponent", eventBus, parentScope) {

    private val users = ConcurrentHashMap<Long, User>()

    override suspend fun onStart(scope: CoroutineScope) {
        subscribe<AuthExpired>(AuthExpired::class)
        subscribe<PersistConfig>(PersistConfig::class)
    }

    override suspend fun wrapEvent(event: Any): AuthMsg? = when (event) {
        is AuthExpired -> OnAuthEvent(event)
        is PersistConfig -> OnAuthEvent(event)
        is CommandEnvelope -> HandleAuthQuery(event)
        else -> null
    }

    fun replaceAll(list: List<User>) {
        users.clear()
        list.forEach { users[it.userId] = it }
    }

    override suspend fun handle(msg: AuthMsg) = when (msg) {
        is LoadUsers -> { msg.users.forEach { users[it.userId] = it }; logger.info("Loaded ${users.size} users") }
        is OnAuthEvent -> when (msg.event) {
            is AuthExpired -> { users.remove(msg.event.userId); logger.info("User ${msg.event.userId} expired, removed") }
            is PersistConfig -> {
                try {
                    File(usersPath).writeText(users.values.joinToString("\n") { it.cookie })
                } catch (e: Exception) {
                    logger.warn("Failed to save users.txt: ${e.message}")
                }
                try {
                    if (mongo.isConnected()) mongo.saveUsers(users.values.toList())
                } catch (e: Exception) {
                    logger.warn("Failed to save users to MongoDB: ${e.message}")
                }
            }
            else -> {}
        }
        is HandleAuthQuery -> handleQuery(msg.env)
    }

    private suspend fun handleQuery(env: CommandEnvelope) {
        val ack = when (env.command) {
            is GetValidPaymentAccount -> validPaymentAccount(env.command.price)
            is DeductCoins -> {
                val u = users[env.command.userId]
                if (u != null) {
                    users[u.userId] = u.copy(coins = u.coins - env.command.amount)
                    OkResponse
                } else ErrorResponse("user not found: ${env.command.userId}")
            }
            else -> ErrorResponse("unknown auth query")
        }
        eventBus.publish(CommandAck(env.id, ack))
    }

    fun validPaymentAccount(coins: Long): List<User> = users.values.filter { it.coins >= coins }
}

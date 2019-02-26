import com.googlecode.objectify.Key
import com.googlecode.objectify.LoadResult
import com.googlecode.objectify.Objectify
import com.googlecode.objectify.ObjectifyService
import com.googlecode.objectify.cmd.LoadType
import com.googlecode.objectify.impl.EntityMetadata
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1

open class DB() {

    open fun ofy() = ObjectifyService.ofy()

    fun <E : Any> getById(type: KClass<E>, id: Long) = ofy().load().type(type.java).id(id)
    fun <E : Any> getByIds(type: KClass<E>, vararg ids: Long) =
        getByKeys(*ids.map { Key.create(type.java, it) }.toTypedArray())

    fun <E : Any> getByName(type: KClass<E>, name: String) = ofy().load().type(type.java).id(name)
    fun <E : Any> getByNames(type: KClass<E>, vararg names: String) = ofy().load().type(type.java).ids(*names)

    fun <E> getByKey(key: Key<E>): LoadResult<E> = ofy().load().key(key)
    fun <E> getByKeys(vararg keys: Key<E>): Map<Key<E>, E> = ofy().load().keys(*keys)

    fun transact(r: () -> Unit) = ofy().transact(r)
    fun transactionless(r: () -> Unit) = ofy().transactionless(r)

    fun put(vararg entities: Any) = ofy().save().entities(*entities)
    fun putNow(vararg entities: Any) = put(*entities).now()

    fun delete(vararg entities: Any) = ofy().delete().entities(*entities)
    fun deleteNow(vararg entities: Any) = delete(*entities).now()

    fun <E : Any> query(type: KClass<E>, where: (Query<E>.() -> Unit) = {}): Query<E> {
        val q = Query<E>(type)
        where(q)
        return q
    }

    inner class Query<E : Any>(private val type: KClass<E>) {
        /**
         * Entity Metadata
         */
        private val meta: EntityMetadata<E> by lazy { ObjectifyService.factory().getMetadata(type.java) }

        /**
         * Filters
         */
        inner class Filter<E>(val prop: KMutableProperty1<E, out Any>, val operator: String, val value: Any)

        private val filters: MutableList<Filter<E>> = ArrayList()
        private fun addFilter(prop: KMutableProperty1<E, out Any>, operator: String, value: Any): Unit {
            filters.add(Filter(prop, operator, value))
        }

        private val projectFields: ArrayList<KMutableProperty1<E, Any>> = ArrayList()
        fun project(vararg fields: KMutableProperty1<E, Any>) = projectFields.addAll(fields.toMutableList())

        fun <V : Any> KMutableProperty1<E, V>.eq(id: V) = addFilter(this, "==", id)
        fun <V : Any> KMutableProperty1<E, V>.lessThanOrEqual(value: V) = addFilter(this, "<=", value)
        fun <V : Any> KMutableProperty1<E, V>.lessThan(value: V) = addFilter(this, "<", value)
        fun <V : Any> KMutableProperty1<E, V>.greaterThanOrEqual(value: V) = addFilter(this, ">=", value)
        fun <V : Any> KMutableProperty1<E, V>.greaterThan(value: V) = addFilter(this, ">", value)

        /**
         * Sort
         */
        inner class Sort<E>(val prop: KMutableProperty1<E, out Any>, val desc: Boolean)

        private var sort: Sort<E>? = null
        fun <V : Any> KMutableProperty1<E, V>.asc(): Sort<E> = Sort(this, false)
        fun <V : Any> KMutableProperty1<E, V>.desc(): Sort<E> = Sort(this, true)
        fun orderBy(sort: Sort<E>): Query<E> {
            this.sort = sort
            return this
        }

        private fun <V : Any> isKeyProperty(prop: KMutableProperty1<E, V>): Boolean =
            meta.keyMetadata.idFieldName.equals(prop.name)

        /**
         * Limit
         */
        private var limit: Int = -1

        fun limit(limit: Int) = { this.limit = limit; this }

        fun toObjectifyQuery(): LoadType<E> {
            val loadType = ofy().load().type(type.java)

            if (0 < projectFields.size) {
                loadType.project(*(projectFields.map { it.name }.toTypedArray()))
            }

            filters.forEach {
                if (isKeyProperty(it.prop)) {
                    loadType.filterKey(it.operator, it.value)
                } else {
                    loadType.filter("${it.prop.name} ${it.operator}", it.value)
                }
            }
            val s = sort
            if (s != null) {
                if (isKeyProperty(s.prop)) {
                    loadType.orderKey(s.desc)
                } else {
                    loadType.order(
                        if (s.desc) {
                            "-" + s.prop.name
                        } else {
                            s.prop.name
                        }
                    )
                }

            }

            if (0 < this.limit) {
                loadType.limit(limit)
            }

            return loadType
        }

        fun asList(): List<E> = toObjectifyQuery().list()
        fun asKeyList(): List<Key<E>> = toObjectifyQuery().keys().list()
    }
}

object db : DB() {
    private object cached : DB() {
        override fun ofy(): Objectify {
            return super.ofy().cache(true)
        }
    }
    fun cache(cache: Boolean=true): DB = if (cache) cached else this
}

class Person {
    var firstName: String = "";
}

fun main() {
    db.query(Person::class) {
        Person::firstName.eq("test")
        orderBy(Person::firstName.desc())
    }
}
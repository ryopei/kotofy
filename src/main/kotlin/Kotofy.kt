import com.googlecode.objectify.Key
import com.googlecode.objectify.LoadResult
import com.googlecode.objectify.ObjectifyService
import com.googlecode.objectify.cmd.LoadType
import com.googlecode.objectify.impl.EntityMetadata
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1

object db {
    fun ofy() = ObjectifyService.ofy()

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
        val q = db.Query<E>(type)
        where(q)
        return q
    }

    class Query<E : Any>(private val type: KClass<E>) {
        /**
         * Entity Metadata
         */
        private val meta: EntityMetadata<E> by lazy { ObjectifyService.factory().getMetadata(type.java) }

        /**
         * Filters
         */
        class Filter<E>(val prop: KMutableProperty1<E, out Any>, val operator: String, val value: Any)

        private val filters: MutableList<Filter<E>> = ArrayList()
        private fun addFilter(prop: KMutableProperty1<E, out Any>, operator: String, value: Any) =
            filters.add(Filter(prop, operator, value))

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
        class Sort<E>(val prop: KMutableProperty1<E, out Any>, val desc: Boolean)

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

        fun limit(limit: Int) = { this@Query.limit = limit; this }

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
            return loadType
        }

        fun list(): List<E> = toObjectifyQuery().list()
        fun keyList(): List<Key<E>> = toObjectifyQuery().keys().list()
    }
}
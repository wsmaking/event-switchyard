package app.ownership
import java.util.concurrent.atomic.AtomicReference

interface Ownership { fun owns(key: String): Boolean }

class OwnedKeysView(private val ref: AtomicReference<Set<String>>) : Ownership {
    override fun owns(key: String) = ref.get().contains(key)
}

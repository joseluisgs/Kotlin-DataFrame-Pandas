import krangl.*
import java.io.File

// http://holgerbrandl.github.io/krangl/faq/
// http://holgerbrandl.github.io/krangl/

fun main() {
    println("Hola Krangl !! (y de paso Pandas!!!)")

    // Ejemplo de Tenistas y Raquetas
    ejemploRaquetasTenistasKrangl()
    // Ejemplo de Almacén
    ejemploAlmacenKrangl()
    // Ejemplo Pokemon desde Internet
    ejemploRickyMortyKrangl()
}

fun ejemploRickyMortyKrangl() {
    // Podemos consultar la lista de personajes de Rick y Morty nos traemos 800 personajes
    val personajes = (1..800).toList().joinToString(",") { it.toString() }
    val data = DataFrame.fromJson("https://rickandmortyapi.com/api/character/$personajes")
    data.schema()
    // Agrupoamos por especies y que esten vivos
    data.groupBy("species", "status").print()
    // Seleccionamos los que esten vivos y que sean humanos y su localizacion sea la tierra
    data
        .filter { it["species"].eq("Human") }
        .filter { it["status"].eq("Alive") }
        .select("name", "status", "species")
        .print()

    // Agrupados por especies y y que estén vivos
    data.groupBy("species", "status")
        .filter { it["status"].eq("Alive") }
        .print()

    // Total de personajes por especie
    data.groupBy("species")
        .summarize("count" to { it["species"].length })
        .print()

    // Total de personajes por especie y que estén vivos
    data.groupBy("species")
        .filter { it["status"].eq("Alive") }
        .summarize("count" to { it["species"].length })
        .print()

}

fun ejemploAlmacenKrangl() {
    // Ejemplos con un CSV de datos  Abiertos de Madrid, con una columna de accidentesue lo hemos trabajado en clase
    val data = DataFrame.readCSV("data/products.csv")

    // Todos los productos
    println(data)
    data.schema()
    // Nombre de los productos
    data.select("productName").print()
    // nombre de los productos con stock mayor de 10
    data.filter { it["unitsInStock"].greaterEqualsThan(10) }.select("productName", "unitsInStock").print()
    // Nombre de los productos con stock menor que 10 ordenado por unidades de stock descendente
    data.filter { it["unitsInStock"].lesserEquals(10) }.select("productName", "unitsInStock")
        .sortedByDescending("unitsInStock")
        .print()
    // Numero de productos agrupados por proveedor ordenados por productos
    data.groupBy("supplierID").summarize("total" to { it["supplierID"].length }).sortedByDescending("total").print()
    // Obtener la suma del precio unitario de todos los productos agrupados por el número de existencias en el almacén odenados por unidades en stock
    data.groupBy("unitsInStock").summarize("suma" to { it["unitPrice"].sum() }).sortedByDescending("unitsInStock")
        .print()
    // Obtener la suma del precio unitario de todos los productos agrupados por el número de existencias en el almacén, pero solo obtener aquellos registros cuya suma sea mayor a 100
    // ordenados por suma
    data.groupBy("unitsInStock").summarize("suma" to { it["unitPrice"].sum() })
        .filter { it["suma"].greaterEqualsThan(100) }
        .sortedByDescending("suma")
        .print()
    // Estadisticas de productos agrupados por proveedor y cuyas unidades en stock sean mayor que 10 ordenados por precio medio
    data.groupBy("supplierID")
        .filter { it["unitsInStock"].greaterEqualsThan(10) }
        .summarize(
            "total" to { it["supplierID"].length },
            "PrecioMedio" to { it["unitPrice"].mean() },
            "PrecioMinimo" to { it["unitPrice"].min() },
            "PrecioMaximo" to { it["unitPrice"].max() },
        )
        .sortedByDescending("PrecioMedio")
        .print()
}

private fun ejemploRaquetasTenistasKrangl() {
    // Leemos el fichero de entrada
    val raquetasData = DataFrame.readCSV("data/raquetas.csv")
    // Imprimimos el DataFrame
    raquetasData.print()
    raquetasData.schema()

    // Seleccionamos campos
    raquetasData.select("id", "marca").print()

    // podemos ordenar los datos
    raquetasData.sortedBy("precio", "peso").print()

    // Consultas más especificas
    raquetasData.filter { it["precio"].greaterEqualsThan(200) }.print()

    // Vamos a añadir una nueva consulta que sea la suma de los pesos de todas las raquetas
    val grupo = raquetasData.select("id", "marca", "precio", "peso").groupBy("marca")

    grupo.sortedBy("marca").print()

    // Podemos obtener estadisticas y ordenar los datos
    val resumen = grupo.summarize(
        "total" to { it["peso"].length },
        "pesoMedio" to { it["peso"].mean() },
        "pesoMax" to { it["precio"].max() },
        "pesoMin" to { it["precio"].min() },
        // "pesoDesv" to { it["precio"].sd() },
        "precioMedio" to { it["precio"].mean() },
    ).sortedBy("pesoMedio")

    resumen.print()

    // Vamos a añadir una nueva columna que sea peso/precio
    val nuevo = raquetasData.addColumn("pesoPrecio") { it["peso"] / it["precio"] }
    nuevo.print()
    // Lo salvamos
    nuevo.writeCSV(File("data/raquetas_out.csv"))
    resumen.writeCSV(File("data/raquetas_resumen.csv"))

    // Tambien podemos leer json
    val json = DataFrame.fromJson("data/tenistas.json")
        .select("nombre", "ranking", "puntos")
        .sortedByDescending("puntos")
    json.print()
    json.writeCSV(File("data/tenistas-out.csv"))
}
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.*
import org.jetbrains.kotlinx.dataframe.io.readCSV
import org.jetbrains.kotlinx.dataframe.io.readJson
import org.jetbrains.kotlinx.dataframe.io.writeCSV
import org.jetbrains.kotlinx.dataframe.io.writeJson
import java.io.File
import java.time.DayOfWeek
import java.util.*

// https://kotlin.github.io/dataframe/gettingstarted.html

// Cuidado con los decimales!!!


fun main() {
    println("Hello, DataFrames")

    // ejemploAlmacenDataFrame()
    // ejemploRickyMortyDataFrame()
    ejemplosAccidentesDataFrame()


}

/**
 * Lo bueno de poder tipar :)
 */
fun ejemplosAccidentesDataFrame() {
    val accidentes = loadAccidentesFromCsv(File("data/accidentes.csv"))
    // Lo cargamos en el dataframe
    val data = accidentes.toDataFrame()
    // Lo mostramos en pantalla
    // println(data.schema())
    println("Filas: ${data.rowsCount()}")

    // Accidentes con alcohol y drogas, ya no neceitamos hacer casting porque todo está tipado
    val alcoholDrogas = data.filter { it.positivoAlcohol && it.positivoDrogas }.rowsCount()
    println("Accidentes con alcohol y drogas: $alcoholDrogas")

    // Accidentes agrupados por sexo
    val accidentesPorSexo = data
        .groupBy { it.sexo }
        .aggregate {
            count() into "total"
        }.filter { it.sexo == "Hombre" || it.sexo == "Mujer" }

    println("Accidentes por sexo:")
    println(accidentesPorSexo)

    // Accidentes por Dias
    val accidentesPorDias = data.groupBy { it.fecha }
        .aggregate {
            count() into "total"
        }

    println("Accidentes por dia:")
    println(accidentesPorDias)

    // Accidentes por tipo de vehiuclo
    val accidentesPorTipoVehiculo = data.groupBy { it.tipoVehiculo }
        .aggregate {
            count() into "total"
        }
    println("Accidentes por tipo de vehiculo:")
    println(accidentesPorTipoVehiculo)

    // Accidentes fines de semana y alcohol
    val accidentesFinesSemana = data
        .filter { it.fecha.dayOfWeek == DayOfWeek.FRIDAY && it.fecha.dayOfWeek == DayOfWeek.SATURDAY || it.fecha.dayOfWeek == DayOfWeek.SUNDAY }
        .filter { it.positivoAlcohol }

    println("Accidentes fines de semana y alcohol:")
    println(accidentesFinesSemana)

    // Estadisticas por distritos
    val accidentesPorDistrito = data.groupBy { it.distrito }
        .aggregate {
            count() into "total"
        }

    println("Accidentes por distrito:")
    println(accidentesPorDistrito)

    // Distrito con mas accidentes
    val distritoConMasAccidentes = accidentesPorDistrito.sortByDesc("total").first()
    println("Distrito con mas accidentes:")
    println(distritoConMasAccidentes)

    // Distrito con menor accidentes
    val distritoConMenosAccidentes = accidentesPorDistrito.sortBy("total").first()
    println("Distrito con menos accidentes:")
    println(distritoConMenosAccidentes)

    // El dia con mas accidentes
    val diaConMasAccidentes = accidentesPorDias.sortByDesc("total").first()
    println("Dia con mas accidentes:")
    println(diaConMasAccidentes)

    // El día con menos accidentes
    val diaConMenosAccidentes = accidentesPorDias.sortBy("total").first()
    println("Dia con menos accidentes:")
    println(diaConMenosAccidentes)

    // avg accident mens
    val totalDias = accidentesPorDias.groupBy { it.fecha }.keys.rowsCount()
    println("Total dias:")
    println(totalDias)

    // Media de accidentes por dia
    val mediaAccidentesPorDia = accidentesPorDias.aggregate {
        mean("total") into "media"
    }
    println("Media de accidentes por dia:")
    println(mediaAccidentesPorDia)

    // Dia donde más mujeres tuvieron un accfidente
    val diaConMasMujeres = data.filter { it.sexo == "Mujer" }.groupBy { it.fecha }
        .aggregate {
            count() into "total"
        }.sortByDesc("total").first()
    println("Dia con mas mujeres:")
    println(diaConMasMujeres)

    // Dia donde mas hombres tuvieron un accidente
    val diaConMasHombres = data.filter { it.sexo == "Hombre" }.groupBy { it.fecha }
        .aggregate {
            count() into "total"
        }.sortByDesc("total").first()

    println("Dia con mas hombres:")
    println(diaConMasHombres)

    val fechaSexo = data.groupBy("fecha", "sexo")
        .aggregate {
            count() into "total"
        }.filter { it.sexo == "Mujer" || it.sexo == "Hombre" }

    println(fechaSexo)

    // media de hombre
    val mediaHombres = fechaSexo.filter { it.sexo == "Hombre" }.aggregate {
        mean("total") into "media"
    }
    println("Media hombres:")
    println(mediaHombres)
    // MEdia de mujeres
    val mediaMujeres = fechaSexo.filter { it.sexo == "Mujer" }.aggregate {
        mean("total") into "media"
    }
    println("Media mujeres:")
    println(mediaMujeres)

    // Accidentes en chamberí con alcohol y motocileta
    val accidentesChamartinAlcohol =
        data.filter { it.distrito == "CHAMBERÍ".uppercase() && it.positivoAlcohol && it.tipoVehiculo.contains("Motocicleta") }
    println("Accidentes en chamartin con alcohol:")
    println(accidentesChamartinAlcohol)
    // Lo podemos guardar en json
    accidentesChamartinAlcohol.writeJson(File("data/accidentes-chamberi.json"))

}

fun ejemploRickyMortyDataFrame() {
    // Podemos consultar la lista de personajes de Rick y Morty nos traemos 800 personajes
    val personajes = (1..800).toList().joinToString(",") { it.toString() }
    val data = DataFrame.readJson("https://rickandmortyapi.com/api/character/$personajes")
    // println(data.schema())

    data.groupBy("species", "status").print()

    // Seleccionamos los que esten vivos y que sean humanos
    data.filter {
        "status"<String>() == "Alive" && "species"<String>() == "Human"
    }
        .select("name", "status", "species")
        .print()

    // total por especie
    data.groupBy("species")
        .aggregate {
            count() into "total"
        }
        .print()

    // total por especie y que esten vivos
    data.groupBy("species")
        .aggregate {
            count {
                "status"<String>() == "Alive"
            } into "total"
        }
        .print()

}

fun ejemploAlmacenDataFrame() {

    Locale.setDefault(Locale.ENGLISH)  // Muy importante pues te detecta los locale!!!

    val data = DataFrame.readCSV(File("data/products.csv"))
    println(data.schema())
    println(data.columnsCount())
    println(data.rowsCount())

    // Nombre de los productus
    data.select("productName").print()

    data.select("unitPrice").print()

    // nombre de los productos con stock mayor de 10
    data.filter { "unitsInStock"<Int>() > 10 }
        .select("productName")
        .print()

    // Nombre de los productos con stock menor que 10 ordenado por unidades de stock descendente
    data.filter { "unitsInStock"<Int>() < 10 }
        .select("productName", "unitsInStock")
        .sortByDesc("unitsInStock")
        .print()

    // Numero de productos agrupados por proveedor ordenados por productos
    data.groupBy("supplierID")
        .aggregate { count() into "total" }
        .sortByDesc("total")
        .print()

    // Obtener la suma del precio unitario de todos los productos agrupados por el número de existencias
    // en el almacén odenados por unidades
    data.groupBy("unitsInStock")
        .aggregate { sum("unitPrice"<Double>()) into "total" }
        .sortByDesc("total")
        .print()

    // Obtener la suma del precio unitario de todos los productos agrupados por el número de existencias en el almacén, pero solo obtener aquellos registros cuya suma sea mayor a 100
    data.groupBy("unitsInStock")
        .aggregate { sum("unitPrice") into "total" }
        .filter { "total"<Double>() > 100 }
        .sortByDesc { it["total"].convertToDouble() }
        .print()

    // Estadisticas de productos agrupados por proveedor y cuyas total de unidades en stock sean mayor que 3 ordenados por precio medio
    data.groupBy("supplierID")
        .aggregate {
            count() into "total"
            max("unitPrice"<Double>()) into "maxPrice"
            min("unitPrice"<Double>()) into "minPrice"
            mean("unitPrice"<Double>()) into "avgPrice"
        }
        .filter { "total"<Int>() > 3 }
        .sortByDesc("avgPrice")
        .print()

    // Obtener el producto cuyo precio supere el precio medio
    // Avg price
    val avgPrice: Double =
        data.aggregate { mean("unitPrice"<Double>()) into "avgPrice" }["avgPrice"].toString().toDouble()
    println("Avg price: $avgPrice")

    val res = data.filter { "unitPrice"<Double>() > avgPrice }
        .select("productName", "unitPrice")

    res.print()

    res.writeCSV(File("data/products_avg_price.csv"))
    res.writeJson(File("data/products_avg_price.json"))


}

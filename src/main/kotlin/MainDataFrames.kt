import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.*
import org.jetbrains.kotlinx.dataframe.io.readCSV
import java.io.File

fun main() {
    println("Hello, DataFrames")

    ejemploAlmacenDataFrame()


}

fun ejemploAlmacenDataFrame() {

    val data = DataFrame.readCSV(File("data/products.csv"))
    println(data.schema())
    println(data.columnsCount())
    println(data.rowsCount())

    // Nombre de los productus
    data.select("productName").print()

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
    data.groupBy("supplierID", "productName")
        .sortBy("supplierID").print()
}

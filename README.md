# Normify

Normify is a rust library for effciently deserializing and transforming JSON data into a tabular format

## Design Overview

Normify's transformation logic is necessarily opinionated. The 8 Normify Rules are as follows:

1. Every JSON Object maps to a single ROW of tabular data
2. Every table has a auto-incrementing Primary Key (PK) Column named **ID** of type _u64_

   `normify::IdColumn`

3. The JSON Key associated with a JSON Object or ObjArray determines the Table to which the ROW/ROWs belong
4. Any JSON Object without a Key is a ROW belonging to the root table
   `normify::type_aliases::RootTable`
5. Any JSON ObjArray without a Key is a collection of one or more ROWS belonging to the **Root Table**
   `normify::type_aliases::RootTable`
6. Every ROW not belonging to the root table has a parent ROW
7. Every ROW with an associated parent ROW must have a Foreign Key (FK) entry relecting the PK of the parent ROW
8. A nested JSON object triggers the creation of a child table with a foreign key column, and the insertion of the parent table's ID into the foreign key column

---

## Transformation Breakdown

> JSON Primitives refer to the JSON data types: String, Number, Bool, and Null

> JSON List refers to a JSON Array where every element is a JSON Primitive of the same data type

> JSON ObjArray refers to a JSON Array where every element is a JSON Object

> The "=>" symbol will be used to mean "maps to; represents"

1. Every JSON Array can be represented as a JSON List or JSON ObjArray ( mixed type Arrays will be homogenized based on [certain conditions](#homogenization-rules) )
2. JSON Primitive => Cell; Key => Column Name
3. JSON List => Cell; Key => Column Name
4. JSON Object => Row; Key => Table Name
5. JSON ObjArray => One or more Rows; Key => Table Name

---

## Homogenization Rules

1. A JSON array comprised entirely of non-objects becomes a single entry in a list column `normify::NestedArray`, after the array is normalized to a uniform type
2. A JSON array of objects is parsed as Rows belonging to the table defined by the array Key
3. A JSON array of both objects and non-objects can either be represented as a `normify::type_aliases::UnionizedTable` or as an entry in a `normify::type_aliases::StringListColumn` depending on user configuration

/*
 * Copyright 2019 Lightbend Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cloudstate.proxy.views

import com.softwaremill.diffx.scalatest.DiffMatcher
import io.cloudstate.proxy.views.CSQL.ColumnSelectPart
import org.scalatest.{EitherValues, Matchers, WordSpec}

class CSQLParserSpec extends WordSpec with Matchers with DiffMatcher with EitherValues {

  import CSQL._

  private def parseSuccess(csql: String): Query =
    CSQL.parse(csql) match {
      case Right(query) => query
      case Left(error) => fail(error.description)
    }

  private def failParsingWith(csql: String, message: String, column: Int) =
    CSQL.parse(csql) match {
      case Right(query) => fail("Expected fail but got " + query)
      case Left(error) =>
        withClue("The error was: " + error.description) {
          error.message should ===(message)
          error.position.column should ===(column)
        }
    }

  private def toAlias(as: String) = Some(as).filter(_.nonEmpty).map(Alias.apply)

  private def select(parts: SelectPart*): Select = Select(parts.toList)

  private def star(table: String = "", as: String = "") = StarSelectPart(toAlias(table), toAlias(as))

  private def toColumnRef(path: String) = ColumnRef(path.split('.').toList.map(PathElement.apply))

  private def column(path: String = "", as: String = "") =
    ColumnSelectPart(toColumnRef(path), toAlias(as))

  private def countStar(as: String = "") = CountSelectPart(toAlias(as))

  private def paramSelect(name: String, as: String = "") = ParameterSelectPart(name, toAlias(as))

  private def from(table: String, as: String = "") = From(table, toAlias(as))

  private def where(conditions: WhereCondition*) = Where(List(WhereAndConditions(conditions.toList)))

  private def compare(op1: Operand, operator: Operator, op2: Operand) =
    WhereCondition(false, op1, CompareOperation(operator, op2))

  private def literal(value: String) = StringOperand(value)
  private def literal(value: Long) = IntegerOperand(value)

  private def col(path: String) = ColumnOperand(toColumnRef(path))

  private def param(name: String) = ParameterOperand(name)

  private def matchQuery(select: Select, from: From, filters: Filter*) = matchTo(Query(select, from, filters.toList))

  "The CSQL Parser" should {
    "parse valid syntax" when {
      "parse a simple select * from statement" in {
        parseSuccess("select * from some_table") should matchQuery(
          select(star()),
          from("some_table")
        )
      }

      "parse an aliased select * from statement" in {
        parseSuccess("select * as field from some_table") should matchQuery(
          select(star(as = "field")),
          from("some_table")
        )
      }

      "parse an aliased table statement" in {
        parseSuccess("select * from some_table as foo") should matchQuery(
          select(star()),
          from("some_table", as = "foo")
        )
      }

      "parse some columns in a select statement" in {
        parseSuccess("select foo, bar from some_table") should matchQuery(
          select(column("foo"), column("bar")),
          from("some_table")
        )
      }

      "parse a column with a deep path and aliased in a select statement" in {
        parseSuccess("select foo.bar.baz as blah from some_table") should matchQuery(
          select(column("foo.bar.baz", as = "blah")),
          from("some_table")
        )
      }

      "parse an input parameter in a select statement" in {
        parseSuccess("select :param as blah from some_table") should matchQuery(
          select(paramSelect("param", as = "blah")),
          from("some_table")
        )
      }

      "parse a quoted values" in {
        parseSuccess("select `fo%o`.`b$ar` as `bl&h` from `s^me_table` as `ali@s`") should matchQuery(
          select(column("fo%o.b$ar", as = "bl&h")),
          from("s^me_table", as = "ali@s")
        )
      }

      "parse a count star select" in {
        parseSuccess("select * as items, count(*) as total from some_table") should matchQuery(
          select(star(as = "items"), countStar(as = "total")),
          from("some_table")
        )
      }

      "parse a where equality to string statement" in {
        parseSuccess("select * from some_table where foo = 'bar'") should matchQuery(
          select(star()),
          from("some_table"),
          where(compare(col("foo"), Equals, literal("bar")))
        )
      }

      "parse a where comparison statement" in {
        parseSuccess("select * from some_table where foo = 'bar' and baz < 10 and blah != :param") should matchQuery(
          select(star()),
          from("some_table"),
          where(
            compare(col("foo"), Equals, literal("bar")),
            compare(col("baz"), LessThan, literal(10)),
            compare(col("blah"), NotEquals, param("param"))
          )
        )
      }

      "parse offset limit syntax" in {
        parseSuccess("select * from some_table offset 10 limit :limitparam") should matchQuery(
          select(star()),
          from("some_table"),
          Offset(ParameterizableNumber.Number(10)),
          Limit(ParameterizableNumber.Parameter("limitparam"))
        )
      }

      "parse offset fetch syntax" in {
        parseSuccess("select * from some_table offset :offsetparam rows fetch next 20 rows only") should matchQuery(
          select(star()),
          from("some_table"),
          Offset(ParameterizableNumber.Parameter("offsetparam")),
          Limit(ParameterizableNumber.Number(20))
        )
      }

    }

    "fail on invalid syntax" when {
      "doesn't start with select" in failParsingWith(
        "create table blah",
        "Expected: select",
        1
      )
      "doesn't have a select parameter" in failParsingWith(
        "select from some_table",
        "Expected: function, column name, parameter or literal",
        8
      )
      "uses an invalid character in a select parameter" in failParsingWith(
        "select f&t from some_table",
        "Unexpected character: &",
        9
      )
      "is missing a from statement" in failParsingWith(
        "select foo",
        "Unexpected end of input",
        11
      )
    }

  }

}

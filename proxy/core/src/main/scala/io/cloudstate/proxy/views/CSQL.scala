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

import java.util.Locale

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.{CharSequenceReader, Position, Positional}

object CSQL {

  trait QueryElement extends Positional

  case class Query(select: Select, from: From, filters: List[Filter])
  case class Alias(name: String) extends QueryElement
  case class PathElement(name: String) extends QueryElement
  case class ColumnRef(path: List[PathElement]) extends QueryElement
  case class Select(parts: List[SelectPart]) extends QueryElement
  sealed trait SelectPart extends QueryElement {
    def as: Option[Alias]
  }
  case class StarSelectPart(table: Option[Alias], as: Option[Alias]) extends SelectPart
  case class ColumnSelectPart(column: ColumnRef, as: Option[Alias]) extends SelectPart
  case class CountSelectPart(as: Option[Alias]) extends SelectPart
  case class ParameterSelectPart(parameter: String, as: Option[Alias]) extends SelectPart

  case class From(table: String, as: Option[Alias]) extends QueryElement

  sealed trait Filter extends QueryElement

  case class Where(or: List[WhereAndConditions]) extends Filter
  case class WhereAndConditions(and: List[WhereCondition]) extends QueryElement
  case class WhereCondition(not: Boolean, operand: Operand, operation: Operation) extends QueryElement

  sealed trait Operation extends QueryElement
  case class CompareOperation(operator: Operator, operand: Operand) extends Operation
  sealed trait Operator
  case object Equals extends Operator
  case object NotEquals extends Operator
  case object GreaterThan extends Operator
  case object GreaterEquals extends Operator
  case object LessThan extends Operator
  case object LessEquals extends Operator
  case class InOperation(operands: List[Operand]) extends Operation
  sealed trait LikeOperation extends Operation
  case class LikeStringOperation(pattern: String) extends LikeOperation
  case class LikeParameterOperation(name: String) extends LikeOperation

  sealed trait Operand extends QueryElement
  case class ColumnOperand(column: ColumnRef) extends Operand
  case class StringOperand(value: String) extends Operand
  case class IntegerOperand(value: Long) extends Operand
  case class FloatOperand(value: Double) extends Operand
  case class ParameterOperand(name: String) extends Operand

  sealed trait ParameterizableNumber extends QueryElement
  object ParameterizableNumber {
    case class Number(value: Long) extends ParameterizableNumber
    case class Parameter(name: String) extends ParameterizableNumber
  }

  case class Offset(offset: ParameterizableNumber) extends Filter
  case class Limit(limit: ParameterizableNumber) extends Filter

  case class CSQLParseError(message: String, csql: String, position: Position) {
    def description =
      s"""$message at ${position.line}:${position.column}
         |${position.longString}
         |""".stripMargin
  }

  def parse(csql: String) = Parser.query.apply(new CharSequenceReader(csql)) match {
    case Parser.Success(query, _) => Right(query)
    case Parser.NoSuccess(msg, next) => Left(CSQLParseError(msg, csql, next.pos))
  }

  private object Parser extends RegexParsers {
    override def skipWhitespace: Boolean = false

    private val ReservedWords = Set(
      "select",
      "count",
      "as",
      "from",
      "inner",
      "outer",
      "full",
      "left",
      "right",
      "join",
      "on",
      "where",
      "and",
      "or",
      "not",
      "in",
      "is",
      "null",
      "true",
      "false",
      "like",
      "order",
      "by",
      "asc",
      "desc",
      "offset",
      "rows",
      "row",
      "fetch",
      "next",
      "first",
      "only",
      "limit"
    )

    private def validate[T](p: Parser[T])(predicate: T => Boolean)(error: T => String) = Parser { in =>
      p(in) match {
        case Success(t, _) if !predicate(t) => Failure(error(t), in)
        case other => other
      }
    }
    private def expect[T](p: Parser[T])(error: Input => String) = Parser { in =>
      p(in) match {
        case Failure(_, next) => Failure(error(next), next)
        case other => other
      }
    }

    private val maybeSpace: Parser[String] = "\\s*".r
    // The error message here is unexpected character because a space is typically required after a term, so if the
    // character is not a space character, then that means an invalid character has been found inside a term.
    private val space: Parser[String] =
      expect("\\s+".r)(in => if (in.atEnd) "Unexpected end of input" else s"Unexpected character: ${in.first}")
    private val commaSep: Parser[String] = "\\s*,\\s*".r
    private def keyword(keyword: String): Parser[String] =
      validate(term)(_.equalsIgnoreCase(keyword))(_ => s"Expected: $keyword")

    private def term: Parser[String] = "[A-Za-z][A-Za-z0-9_]*".r
    private def rawRef =
      validate(term)(t => !ReservedWords(t.toLowerCase(Locale.ROOT)))(reserved => s"$reserved is a reserved word")
    private def quotedRef: Parser[String] = '`' ~>! "[^`]+".r <~! '`'
    private def ref = quotedRef | rawRef

    // todo: support escape sequences in strings
    private def string = '\'' ~>! "[^']*".r <~! '\''
    // other radixes eg hex?
    private def integer: Parser[Long] = "-? *[0-9]+".r.map(_.toLong)
    // support scientific notation?
    private def float: Parser[Double] = "-? *[0-9]+(?:\\.[0-9]+)".r.map(_.toDouble)
    private def parameter = ':' ~>! ref
    private def parameterizableNumberNumber = positioned(integer.map(ParameterizableNumber.Number.apply))
    private def parameterizableNumberParameter = positioned(parameter.map(ParameterizableNumber.Parameter.apply))
    private def parameterizableNumber = parameterizableNumberNumber | parameterizableNumberParameter

    private def alias = positioned(ref map Alias.apply)
    private def asAlias = keyword("as") ~> space ~> alias
    private def pathElement = positioned(ref map PathElement.apply)
    private def columnRef = positioned(rep1sep(pathElement, '.') map ColumnRef.apply)

    private def columnOperand = positioned(columnRef.map(ColumnOperand.apply))
    private def stringOperand = positioned(string.map(StringOperand.apply))
    private def integerOperand = positioned(integer.map(IntegerOperand.apply))
    private def floatOperand = positioned(float.map(FloatOperand.apply))
    private def parameterOperand = positioned(parameter.map(ParameterOperand.apply))
    private def operand: Parser[Operand] =
      (columnOperand | parameterOperand | stringOperand | floatOperand | integerOperand)
        .withErrorMessage("Expected operand")

    private def operatorParser(string: String, operator: Operator) = literal(string).map(_ => operator)
    private def equals = operatorParser("=", Equals)
    private def notEquals = operatorParser("!=", NotEquals)
    private def greaterThan = operatorParser(">", GreaterThan)
    private def greaterEquals = operatorParser(">=", GreaterEquals)
    private def lessThan = operatorParser("<", LessThan)
    private def lessEquals = operatorParser("<=", LessEquals)
    private def operator = equals | notEquals | greaterThan | greaterEquals | lessThan | lessEquals

    private def compareOperation =
      maybeSpace ~> positioned((operator <~ maybeSpace) ~! operand map {
        case operator ~ operand => CompareOperation(operator, operand)
      })
    private def inOperation =
      space ~> positioned(
        keyword("in") ~>! maybeSpace ~> "(" ~>! rep1sep(operand, commaSep) <~ ")" map InOperation.apply
      )
    private def likeOperation =
      space ~> positioned(
        keyword("like") ~>! maybeSpace ~>
        (string.map(LikeStringOperation.apply) | parameter.map(LikeParameterOperation.apply))
      )
    private def operation: Parser[Operation] = compareOperation | inOperation | likeOperation
    // allow parenthesis here and around and conditions?
    private def whereCondition =
      positioned((opt(keyword("not") <~ space) ~ operand ~ operation).map {
        case maybeNot ~ operand ~ operation => WhereCondition(maybeNot.isDefined, operand, operation)
      })
    private def whereAndConditions =
      positioned(rep1sep(whereCondition, maybeSpace ~ keyword("and") ~ space) map WhereAndConditions.apply)
    private def where =
      positioned(
        keyword("where") ~> space ~>
        rep1sep(whereAndConditions, maybeSpace ~ keyword("or") ~ space).map(Where.apply)
      )

    private def from =
      positioned((keyword("from") ~> space ~> ref ~ opt(space ~> asAlias)).map {
        case table ~ alias => From(table, alias)
      })

    private def columnSelectPart =
      positioned((columnRef ~ opt(space ~> asAlias)) map {
        case columnRef ~ as => ColumnSelectPart(columnRef, as)
      })
    private def starSelectPart =
      positioned(((opt(alias <~ '.') <~ '*') ~ opt(space ~> asAlias)) map {
        case table ~ alias => StarSelectPart(table, alias)
      })
    private def countSelectPart =
      positioned(keyword("count") ~> "(*)" ~> opt(space ~> asAlias).map(CountSelectPart.apply))
    private def parameterSelectPart =
      positioned((parameter ~ opt(space ~> asAlias)) map {
        case parameter ~ alias => ParameterSelectPart(parameter, alias)
      })
    private def selectPart =
      (countSelectPart | columnSelectPart | starSelectPart | parameterSelectPart)
        .withFailureMessage("Expected: function, column name, parameter or literal")
    private def select = positioned((keyword("select") ~> space ~> rep1sep(selectPart, commaSep)) map Select.apply)

    private def offset =
      positioned(
        keyword("offset") ~> space ~> parameterizableNumber.map(Offset.apply) <~
        opt(space <~ keyword("rows") | keyword("row"))
      )
    private def limit = positioned(
      (keyword("limit") ~> space ~> parameterizableNumber.map(Limit.apply)) |
      (keyword("fetch") ~> space ~> (keyword("next") | keyword("first")) ~> space ~>
      parameterizableNumber.map(Limit.apply) <~ space <~ (keyword("rows") | keyword("row")) <~ space <~ keyword("only"))
    )

    private def filters = offset | limit | where

    private def endOfInput = Parser { in =>
      if (!in.atEnd) {
        Error(s"Unexpected term or character at end of statement", in)
      } else {
        Success(None, in)
      }
    }

    val query: Parser[Query] = {
      ((select <~ space) ~ from ~ rep(space ~> filters) <~ maybeSpace <~
      endOfInput.withFailureMessage("Expected one of where, offset, limit or fetch")) map {

        case select ~ from ~ filters => Query(select, from, filters)
      }
    }
  }

}

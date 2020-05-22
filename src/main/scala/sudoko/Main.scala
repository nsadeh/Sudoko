package sudoko

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import play.api.libs.json.Json
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import sudoko.Coordinator._
import sudoko.PartConstructor._
import sudoko.BoardValidator.BadBoardLength
import sudoko.BoardValidator.ValidBoardLength
import sudoko.BoardValidator.RawBoard
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.util.Try
import sudoko.PartValidator.FailedValidation
import akka.actor.PoisonPill

object BoardValidator {

    type BoardAlias = Array[Array[String]]

    final case class RawBoard(board: String)
    final case class ValidBoardLength(index: Int, board: Array[String]) extends CoordinatorCommand
    final case class BadBoardLength(boardLength: Int) extends CoordinatorCommand

    def apply(main: ActorRef[CoordinatorCommand], constructor: ActorRef[CoordinatorCommand]): Behavior[RawBoard] = Behaviors.receive { (context, message) => 
        context.log.info("Received raw board")
        val parsed = readBoard(message.board)
        if (correctBoardLength(parsed)) (0 until parsed.length).foreach(idx => actOnRow(idx, parsed(idx), constructor, main)) else main ! BadBoardLength(parsed.size)
        Behaviors.same  
    }


    def readBoard(b: String): BoardAlias = Json.parse(b).as[BoardAlias]
    def correctBoardLength(b: Array[_]) = b.size == 9
    def actOnRow(index: Int, row: Array[String], validRef: ActorRef[ValidBoardLength], invalidRef: ActorRef[BadBoardLength]) = if (correctBoardLength(row)) {
        validRef ! ValidBoardLength(index, row)
    } else {
        invalidRef ! BadBoardLength(row.size)
    }
}

object PartConstructor {

    type Columns = Array[List[Option[Int]]]
    type Squares = Array[List[Option[Int]]]
    case class State(row: List[Option[Int]], columns: Columns, squares: Squares)
    val emptyState = {
        val parts = Array.fill[List[Option[Int]]](9)(List())
        State(List(), parts, parts)
    }
    case class Part(state: State) extends CoordinatorCommand

    def apply(coordinator: ActorRef[CoordinatorCommand]): Behavior[ValidBoardLength] = constructPart(emptyState, coordinator)

    def constructPart(state: State, coordinator: ActorRef[CoordinatorCommand]): Behavior[ValidBoardLength] = Behaviors.receive{ (context, message) =>
        // context.log.info(state.columns.mkString)
        val newState = updateState(state, message)
        coordinator ! Part(state)
        context.log.info(newState.columns.mkString)
        constructPart(newState, coordinator)
    }

    def makeCollection(row: Array[String]): List[Option[Int]] = row.map { case x: String => Try(x.toInt).toOption }.toList
    def updateSquares(squares: Squares, row: List[Option[Int]]): Squares = {
        val grouped = row.grouped(3) zip squares
        (grouped map { case (a, b) => a ::: b}).toArray

    }
    def updateSquares(squares: Squares, row: List[Option[Int]], idx: Int): Squares = Math.floor(idx / 3).toInt match {
        case 0 => updateSquares(squares.slice(0, 3), row)
        case 1 => updateSquares(squares.slice(3, 6), row)
        case 2 => updateSquares(squares.slice(6, 9), row)
    }
    
    def updateState(state: State, row: ValidBoardLength): State = {
        val parsedRow = makeCollection(row.board)
        val columns: Columns = (parsedRow, state.columns).zipped.map {
            case (r, c) => r :: c
        }.toArray
        val squares = updateSquares(state.squares, parsedRow, row.index)
        return State(parsedRow, columns, squares)
    }
}

object PartValidator {

    implicit def checkForDuplicates(xs: List[Option[Int]]) = new {
        def hasDuplicates(list: List[Option[Int]] = xs, seen: Set[Int] = Set[Int]()): Boolean = xs match {
            case head :: tail if head.isDefined => if (seen contains head.get) true else hasDuplicates(tail, seen + head.get)
            case head :: tail if !head.isDefined => hasDuplicates(tail, seen)
            case  _ => false
        }
    }
    case class FailedValidation(message: String) extends CoordinatorCommand
    case class Complete() extends CoordinatorCommand

    def apply(coordinator: ActorRef[CoordinatorCommand]): Behavior[ValidateParts] = Behaviors.receive{ (context, message) => 
        val validRow = message.parts.row.hasDuplicates()
        val validColumns = message.parts.columns.map(_.hasDuplicates()).fold(true)( _ & _ )
        val validSquares = message.parts.squares.map(_.hasDuplicates()).fold(true)( _ & _)
        if (validRow | validColumns | validSquares) coordinator ! FailedValidation("Invalid part!")
        context.log.info(message.parts.squares.mkString)
        val thisIsTheEnd = message.parts.columns.foldLeft(0)( _ + _.size) == 81
        if (thisIsTheEnd) coordinator ! Complete()
        Behaviors.same
    }
}

object Coordinator { 

    import PartValidator._

    trait CoordinatorCommand
    final case class Run(rawBoard: String) extends CoordinatorCommand
    case class ValidateParts(parts: State)

    def apply(): Behavior[CoordinatorCommand] = Behaviors.setup { context =>
        val boardValidator = context.spawn(BoardValidator(context.self, context.self), "boardValidator")
        val partsContstructor = context.spawn(PartConstructor(context.self), "partsConstructor")
        val validator = context.spawn(PartValidator(context.self), "partValidator")

        Behaviors.receiveMessage {
            case Run(board) => boardValidator ! RawBoard(board); Behaviors.same
            case BadBoardLength(int) => context.log.error("Board setup incorrect!"); Behaviors.stopped
            case ValidBoardLength(idx, row) => context.log.info("received valid board row!"); partsContstructor ! ValidBoardLength(idx, row); Behaviors.same
            case Part(state) => context.log.info("Received part!"); validator ! ValidateParts(state); Behaviors.same
            case FailedValidation(message) => context.log.error(message); Behaviors.stopped
            case Complete() => context.log.info("Sudoko config is valid!"); Behaviors.stopped
        }
    }
}

object Main {

    val valid = "[" +
          "[\"5\",\"3\",\".\",\".\",\"7\",\".\",\".\",\".\",\".\"]," +
            "[\"6\",\".\",\".\",\"1\",\"9\",\"5\",\".\",\".\",\".\"]," +
            "[\".\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
            "[\"8\",\".\",\".\",\".\",\"6\",\".\",\".\",\".\",\"3\"]," +
            "[\"4\",\".\",\".\",\"8\",\".\",\"3\",\".\",\".\",\"1\"]," +
            "[\"7\",\".\",\".\",\".\",\"2\",\".\",\".\",\".\",\"6\"]," +
            "[\".\",\"6\",\".\",\".\",\".\",\".\",\"2\",\"8\",\".\"]," +
            "[\".\",\".\",\".\",\"4\",\"1\",\"9\",\".\",\".\",\"5\"]," +
            "[\".\",\".\",\".\",\".\",\"8\",\".\",\".\",\"7\",\"9\"]" +
        "]";

    val tooFewRows = "[" +
                "[\"5\",\"3\",\".\",\".\",\".\",\".\",\"7\",\".\",\".\"]," +
                "[\"6\",\".\",\".\",\"1\",\"9\",\"5\",\".\",\".\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"8\",\".\",\".\",\".\",\"6\",\".\",\".\",\".\",\"3\"]," +
                "[\"4\",\".\",\".\",\"8\",\".\",\"3\",\".\",\".\",\"1\"]," +
                "[\"7\",\".\",\".\",\".\",\"2\",\".\",\".\",\".\",\"6\"]," +
                "[\".\",\"6\",\".\",\".\",\".\",\".\",\"2\",\"8\",\".\"]," +
                "[\".\",\".\",\".\",\".\",\"8\",\".\",\".\",\"7\",\"9\"]" +
                "]"

    val map = Map("valid" -> valid, "tooFewRows" -> tooFewRows)

    def main(args: Array[String]) {
        val system: ActorSystem[Coordinator.CoordinatorCommand] = ActorSystem(Coordinator(), "SudokoSystem")
        system ! Run(map.get(args(0)).get)
    }
}
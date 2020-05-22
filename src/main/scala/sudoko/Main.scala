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
import scala.util.Success
import scala.util.Failure
import sudoko.BoardValidator.InvalidJson

object BoardValidator {

    type BoardAlias = Array[Array[String]]

    final case class RawBoard(board: String)
    final case class ValidBoardLength(index: Int, board: Array[String]) extends CoordinatorCommand
    final case class BadBoardLength(boardLength: Int) extends CoordinatorCommand
    final case class InvalidJson() extends CoordinatorCommand

    /**
      * This actor's job is to ensure that the raw string Sudoko board has the correct size to work. If it does, it returns it to the message hub for further processing.
      *
      * @param main
      * @param constructor
      * @return
      */
    def apply(main: ActorRef[CoordinatorCommand], constructor: ActorRef[CoordinatorCommand]): Behavior[RawBoard] = Behaviors.receive { (context, message) => 
        context.log.info("Received raw board")
        Try(readBoard(message.board)) match {
            case Success(parsed) => if (correctBoardLength(parsed)) (0 until parsed.length).foreach(idx => actOnRow(idx, parsed(idx), constructor, main)) else main ! BadBoardLength(parsed.size)
            case Failure(exception) => main ! InvalidJson()
        }
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

    /**
      * This actor builds the components of the board out of raw strings. It receives a message containing the index of the row and the row as a string list,
      * and sends a state (collection of parts, not guaranteed to be fully formed).
      * 
      * It is a stateful actor, as we want to add to the previous picture of columns and squares as they come up. State is kept by updating the Behavior function.
      *
      * @param coordinator - the message hub
      * @return
      */
    def apply(coordinator: ActorRef[CoordinatorCommand]): Behavior[ValidBoardLength] = constructPart(emptyState, coordinator)

    def constructPart(state: State, coordinator: ActorRef[CoordinatorCommand]): Behavior[ValidBoardLength] = Behaviors.receive{ (context, message) =>
        // context.log.info(state.columns.mkString)
        val newState = updateState(state, message)
        coordinator ! Part(newState)
        // context.log.info(newState.columns.mkString)
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

    /**
      * implicits are a scala feature allowing extensions of APIs from outside the source code (among other things), so I can actually call
      * list.hasDuplicates() on any list of type List[Option[Int]]
      *
      * @param xs - the list to check
      * @return true if contains duplicates, false otherwise
      */
    implicit def checkForDuplicatesOrInvalidDigits(xs: List[Option[Int]]) = new {
        def hasDuplicatesOrInvalidDigits(list: List[Option[Int]] = xs, seen: Set[Int] = Set[Int]()): Boolean = list match {
            case head :: tail if !head.isEmpty => {
                if (head.get > 9) true
                else if (seen contains head.get) true 
                else hasDuplicatesOrInvalidDigits(tail, seen + head.get)
            }
            case head :: tail if head.isEmpty => hasDuplicatesOrInvalidDigits(tail, seen)
            case  _ => false
        }
    }
    case class FailedValidation(message: String) extends CoordinatorCommand
    case class Complete() extends CoordinatorCommand
    case class Status(s: String) extends CoordinatorCommand

    /**
      * This actor is responsible to make sure all the parts it receives are valid. If they are not, it shuts them down, and if the parts are complete
      * (most easily measures as all nine slots in the column are filled) it ends the computation.
      *
      * @param coordinator - the message hub
      * @return
      */
    def apply(coordinator: ActorRef[CoordinatorCommand]): Behavior[ValidateParts] = Behaviors.receive{ (context, message) =>
        val a  = message.parts.squares
        val validRow = message.parts.row.hasDuplicatesOrInvalidDigits()
        val validColumns = message.parts.columns.map(_.hasDuplicatesOrInvalidDigits()).fold(false)( _ | _ )
        val validSquares = message.parts.squares.map(_.hasDuplicatesOrInvalidDigits()).fold(false)( _ | _)
        if (validRow | validColumns | validSquares) coordinator ! FailedValidation("Invalid part!")
        val thisIsTheEnd = message.parts.columns.foldLeft(0)( _ + _.size) == 81
        if (thisIsTheEnd) coordinator ! Complete()
        coordinator ! Status("Last state is valid, moving to the next")
        Behaviors.same
    }
}

object Coordinator { 

    trait CoordinatorCommand
    final case class Run(rawBoard: String) extends CoordinatorCommand
    case class ValidateParts(parts: State)

    /**
      * This actor runs the system. Currently, it also serves as the message hub. This will change in the future, as many messages do not need
      * to go through this intermediary. 
      * 
      * Another improvement is to scale the number of partsConstructor and PartValidators dynamically to increase parallelism
      *
      * @return 
      */
    def apply(): Behavior[CoordinatorCommand] = Behaviors.setup { context =>
        val boardValidator = context.spawn(BoardValidator(context.self, context.self), "boardValidator")
        val partsContstructor = context.spawn(PartConstructor(context.self), "partsConstructor")
        val validator = context.spawn(PartValidator(context.self), "partValidator")

        Behaviors.receiveMessage {
            case Run(board) => boardValidator ! RawBoard(board); Behaviors.same
            case InvalidJson() => context.log.error("Invalid Json"); Behaviors.stopped
            case BadBoardLength(int) => context.log.error("Board setup incorrect!"); Behaviors.stopped
            case ValidBoardLength(idx, row) => context.log.info("received valid board row!"); partsContstructor ! ValidBoardLength(idx, row); Behaviors.same
            case Part(state) => context.log.info("Received part!"); validator ! ValidateParts(state); Behaviors.same
            case FailedValidation(message) => context.log.error(message); Behaviors.stopped
            case PartValidator.Complete() => context.log.info("Sudoko config is valid!"); Behaviors.stopped
            case PartValidator.Status(st) => context.log.info(st); Behaviors.same
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
    
    val invalidJson =   "||[" +
                "[\"5\",\"3\",\".\",\".\",\".\",\".\",\"7\",\".\",\".\"]," +
                "[\"6\",\".\",\".\",\"1\",\"9\",\"5\",\".\",\".\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"8\",\".\",\".\",\".\",\"6\",\".\",\".\",\".\",\"3\"]," +
                "[\"4\",\".\",\".\",\"8\",\".\",\"3\",\".\",\".\",\"1\"]," +
                "[\"7\",\".\",\".\",\".\",\"2\",\".\",\".\",\".\",\"6\"]," +
                "[\".\",\"6\",\".\",\".\",\".\",\".\",\"2\",\"8\",\".\"]," +
                "[\".\",\".\",\".\",\".\",\"8\",\".\",\".\",\"7\",\"9\"]" +
                "]";
        val illegalValue =  "[" +
                "[\"5\",\"3\",\".\",\".\",\".\",\".\",\"7\",\".\",\".\"]," +
                "[\".\",\"6\",\".\",\".\",\".\",\".\",\"1\",\"9\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"8\",\".\",\".\",\".\",\"16\",\".\",\".\",\".\",\"3\"]," +
                "[\"4\",\".\",\".\",\"8\",\".\",\"3\",\".\",\".\",\"1\"]," +
                "[\"7\",\".\",\".\",\".\",\"2\",\".\",\".\",\".\",\"6\"]," +
                "[\".\",\"6\",\".\",\".\",\".\",\".\",\"2\",\"8\",\".\"]," +
                "[\".\",\".\",\".\",\".\",\"8\",\".\",\".\",\"7\",\"9\"]" +
                "]"

        val jaggedRows =   "[" +
                "[\"5\",\"3\",\".\",\".\",\".\",\".\",\"7\",\".\",\".\"]," +
                "[\"6\",\".\",\".\",\"1\",\"9\",\"5\",\".\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"8\",\".\",\".\",\".\",\"6\",\".\",\".\",\".\",\"3\"]," +
                "[\"4\",\".\",\".\",\"8\",\".\",\"3\",\".\",\".\",\"1\"]," +
                "[\"7\",\".\",\".\",\".\",\"2\",\".\",\".\",\".\",\"6\"]," +
                "[\".\",\"6\",\".\",\".\",\".\",\".\",\"2\",\"8\",\".\"]," +
                "[\".\",\".\",\".\",\".\",\"8\",\".\",\".\",\"7\",\"9\"]" +
                "]"

        val tooManyRows =   "[" +
                "[\"5\",\"3\",\".\",\".\",\".\",\".\",\"7\",\".\",\".\"]," +
                "[\"6\",\".\",\".\",\"1\",\"9\",\"5\",\".\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"8\",\".\",\".\",\".\",\"6\",\".\",\".\",\".\",\"3\"]," +
                "[\"4\",\".\",\".\",\"8\",\".\",\"3\",\".\",\".\",\"1\"]," +
                "[\"7\",\".\",\".\",\".\",\"2\",\".\",\".\",\".\",\"6\"]," +
                "[\".\",\"6\",\".\",\".\",\".\",\".\",\"2\",\"8\",\".\"]," +
                "[\".\",\".\",\".\",\".\",\"8\",\".\",\".\",\"7\",\"9\"]" +
                "]"

        val duplicateSquare = "[" +
                "[\"5\",\"3\",\".\",\".\",\".\",\".\",\"7\",\".\",\".\"]," +
                "[\"6\",\".\",\".\",\"1\",\"9\",\"5\",\".\",\".\",\".\"]," +
                "[\"3\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"8\",\".\",\".\",\".\",\"6\",\".\",\".\",\".\",\"3\"]," +
                "[\"4\",\".\",\".\",\"8\",\".\",\"3\",\".\",\".\",\"1\"]," +
                "[\"7\",\".\",\".\",\".\",\"2\",\".\",\".\",\".\",\"6\"]," +
                "[\".\",\"6\",\".\",\".\",\".\",\".\",\"2\",\"8\",\".\"]," +
                "[\".\",\".\",\".\",\"4\",\"1\",\"9\",\".\",\".\",\"5\"]," +
                "[\".\",\".\",\".\",\".\",\"8\",\".\",\".\",\"7\",\"9\"]" +
                "]";

        val duplicateRow = "[" +
                "[\"5\",\"3\",\".\",\".\",\"7\",\".\",\"7\",\".\",\".\"]," +
                "[\"6\",\".\",\".\",\"1\",\"9\",\"5\",\".\",\".\",\".\"]," +
                "[\".\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"8\",\".\",\".\",\".\",\"6\",\".\",\".\",\".\",\"3\"]," +
                "[\"4\",\".\",\".\",\"8\",\".\",\"3\",\".\",\".\",\"1\"]," +
                "[\"7\",\".\",\".\",\".\",\"2\",\".\",\".\",\".\",\"6\"]," +
                "[\".\",\"6\",\".\",\".\",\".\",\".\",\"2\",\"8\",\".\"]," +
                "[\".\",\".\",\".\",\"4\",\"1\",\"9\",\".\",\".\",\"5\"]," +
                "[\".\",\".\",\".\",\".\",\"8\",\".\",\".\",\"7\",\"9\"]" +
                "]";

        val duplicateCol = "[" +
                "[\"5\",\"3\",\".\",\".\",\"7\",\".\",\".\",\".\",\".\"]," +
                "[\"6\",\".\",\".\",\"1\",\"9\",\"5\",\".\",\".\",\".\"]," +
                "[\".\",\"9\",\"8\",\".\",\".\",\".\",\".\",\"6\",\".\"]," +
                "[\"8\",\".\",\".\",\".\",\"6\",\".\",\".\",\".\",\"3\"]," +
                "[\"4\",\".\",\".\",\"8\",\".\",\"3\",\".\",\".\",\"1\"]," +
                "[\"7\",\".\",\".\",\".\",\"2\",\".\",\".\",\".\",\"6\"]," +
                "[\".\",\"6\",\".\",\".\",\".\",\".\",\"2\",\"8\",\"9\"]," +
                "[\".\",\".\",\".\",\"4\",\"1\",\"9\",\".\",\".\",\"5\"]," +
                "[\".\",\".\",\".\",\".\",\"8\",\".\",\".\",\"7\",\"9\"]" +
                "]";

    val map = Map("valid" -> valid, "tooFewRows" -> tooFewRows, "invalidJson" -> invalidJson, "illegalValue" -> illegalValue,
        "jaggedRows" -> jaggedRows, "tooManyRows" -> tooManyRows, "duplicateSquare" -> duplicateSquare, "duplicateRow" -> duplicateRow,
        "duplicateCol" -> duplicateCol)

    def main(args: Array[String]) {
        // spawn an actor system
        val system: ActorSystem[Coordinator.CoordinatorCommand] = ActorSystem(Coordinator(), "SudokoSystem")
        // fire the first command
        system ! Run(map.get(args(0)).get)
    }
}

package sudoko

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import play.api.libs.json.Json
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import sudoko.Coordinator._
import sudoko.PartConstructor._
import sudoko.BoardValidator.RawBoard
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.util.Try
import akka.actor.PoisonPill
import scala.util.Success
import scala.util.Failure
import sudoko.Commands.SudokoCommands

object BoardValidator {

    type BoardAlias = Array[Array[String]]

    final case class RawBoard(sudoko: ActorRef[SudokoCommands], board: String)

    /**
      * This actor's job is to ensure that the raw string Sudoko board has the correct size to work. If it does, it returns it to the message hub for further processing.
      *
      * @param main
      * @param constructor
      * @return
      */
    def apply(): Behavior[RawBoard] = Behaviors.receive { (context, message) => 
        context.log.info("Received raw board")
        Try(readBoard(message.board)) match {
            case Success(parsed) => if (correctBoardLength(parsed)) (0 until parsed.length).foreach(idx => actOnRow(idx, parsed(idx), message.sudoko)) else message.sudoko ! Commands.InvalidBoardLength()
            case Failure(exception) => message.sudoko ! Commands.InvalidJson()
        }
        Behaviors.same  
    }


    def readBoard(b: String): BoardAlias = Json.parse(b).as[BoardAlias]
    def correctBoardLength(b: Array[_]) = b.size == 9
    def actOnRow(index: Int, row: Array[String], entity: ActorRef[SudokoCommands]) = if (correctBoardLength(row)) {
        entity ! Commands.RawSudokoRow(index, row)
    } else {
        entity ! Commands.InvalidBoardLength()
    }
}

object PartConstructor {

    case class UpdateState(idx: Int, row: Array[String])

    /**
      * This actor builds the components of the board out of raw strings. It receives a message containing the index of the row and the row as a string list,
      * and sends a state (collection of parts, not guaranteed to be fully formed).
      * 
      * It is a stateful actor, as we want to add to the previous picture of columns and squares as they come up. State is kept by updating the Behavior function.
      *
      * @param coordinator - the message hub
      * @return
      */
    def apply(ref: ActorRef[SudokoCommands]): Behavior[UpdateState] = constructPart(Sudoko.emptyState, ref)
    
    // Behaviors.receive{ (context, message) =>
    //     val newState = updateState(message.oldState, message.idx, message.row)
    //     message.ref ! Commands.ConstructedState(newState)
    //     Behaviors.same
    // }

    def constructPart(state: Sudoko.State, ref: ActorRef[SudokoCommands]): Behavior[UpdateState] = Behaviors.receive{ (context, message) =>
        // context.log.info(state.columns.mkString)
        val newState = updateState(state, message.idx, message.row)
        ref ! Commands.ConstructedState(newState)
        // context.log.info(newState.columns.mkString)
        constructPart(newState, ref)
    }

    def makeCollection(row: Array[String]): List[Option[Int]] = row.map { case x: String => Try(x.toInt).toOption }.toList
    def updateSquares(squares: Sudoko.Squares, row: List[Option[Int]]): Sudoko.Squares = {
        val grouped = row.grouped(3) zip squares
        (grouped map { case (a, b) => a ::: b}).toArray

    }
    def updateSquares(squares: Sudoko.Squares, row: List[Option[Int]], idx: Int): Sudoko.Squares = Math.floor(idx / 3).toInt match {
        case 0 => updateSquares(squares.slice(0, 3), row)
        case 1 => updateSquares(squares.slice(3, 6), row)
        case 2 => updateSquares(squares.slice(6, 9), row)
    }
    
    def updateState(state: Sudoko.State, idx: Int, row: Array[String]): Sudoko.State = {
        val parsedRow = makeCollection(row)
        val columns: Sudoko.Columns = (parsedRow, state.columns).zipped.map {
            case (r, c) => r :: c
        }.toArray
        val squares = updateSquares(state.squares, parsedRow, idx)
        return Sudoko.State(parsedRow, columns, squares)
    }
}

object PartValidator {

    case class ValidateState(ref: ActorRef[SudokoCommands], state: Sudoko.State)

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

    /**
      * This actor is responsible to make sure all the parts it receives are valid. If they are not, it shuts them down, and if the parts are complete
      * (most easily measures as all nine slots in the column are filled) it ends the computation.
      *
      * @param coordinator - the message hub
      * @return
      */
    def apply(): Behavior[ValidateState] = Behaviors.receive{ (context, message) =>
        val validRow = message.state.row.hasDuplicatesOrInvalidDigits()
        val validColumns = message.state.columns.map(_.hasDuplicatesOrInvalidDigits()).fold(false)( _ | _ )
        val validSquares = message.state.squares.map(_.hasDuplicatesOrInvalidDigits()).fold(false)( _ | _)
        if (validRow | validColumns | validSquares) message.ref ! Commands.InvalidState()
        val thisIsTheEnd = message.state.columns.foldLeft(0)( _ + _.size) == 81
        if (thisIsTheEnd) message.ref ! Commands.Complete()
        message.ref ! Commands.Status("Last state is valid, moving to the next")
        Behaviors.same
    }
}

class Sudoko

object Sudoko {

    type Columns = Array[List[Option[Int]]]
    type Squares = Array[List[Option[Int]]]
    case class State(row: List[Option[Int]], columns: Columns, squares: Squares)
    val emptyState = {
        val parts = Array.fill[List[Option[Int]]](9)(List())
        State(List(), parts, parts)
    }

    import Commands._

    def apply(coordinator: ActorRef[CoordinatorCommand], boardValidator: ActorRef[RawBoard], validator: ActorRef[PartValidator.ValidateState]): Behavior[SudokoCommands] = 
        Behaviors.setup { context =>
            val partsConstructor = context.spawn(PartConstructor(context.self), context.self.path.name + "partsConstructor")
            Behaviors.receiveMessage { msg =>
                msg match {
                    case Run(board) => context.log.info("Starting board " + context.self.path.name); boardValidator ! RawBoard(context.self, board); Behaviors.same
                    case RawSudokoRow(idx, row) => context.log.info(context.self.path.name + " received a row!"); partsConstructor ! UpdateState(idx, row); Behaviors.same
                    case ConstructedState(state) => context.log.info(context.self.path.name + " received state"); validator ! PartValidator.ValidateState(context.self, state); Behaviors.same
                    case Status(msg) => context.log.info(context.self.path.name + " Status: " + msg); Behaviors.same
                    case Complete() => context.log.info("SUCCESS! is a valid Sudoko configuration!"); coordinator ! Done(); Behaviors.stopped

                    case InvalidBoardLength() => context.log.error(context.self.path.name + " has the wrong dimensions"); coordinator ! Done(); Behaviors.stopped
                    case InvalidJson() => context.log.error(context.self.path.name + " Failed Json validation!"); coordinator ! Done(); Behaviors.stopped
                    case InvalidState() => context.log.error(context.self.path.name + " has invalid Sudoko configuration!"); coordinator ! Done(); Behaviors.stopped
                
            }
        }
    }
}

object Commands {

    // all the command Sudoko receives
    sealed trait SudokoCommands

    // valid cases
    final case class Run(board: String) extends SudokoCommands
    final case class RawSudokoRow(idx: Int, row: Array[String]) extends SudokoCommands
    final case class ConstructedState(state: Sudoko.State) extends SudokoCommands
    final case class Status(s: String) extends SudokoCommands
    final case class Complete() extends SudokoCommands

    // invalid - shut down actor
    final case class InvalidBoardLength() extends SudokoCommands
    final case class InvalidJson() extends SudokoCommands
    final case class InvalidState() extends SudokoCommands

}

object Coordinator { 

    trait CoordinatorCommand
    final case class Start(name: String, rawBoard: String) extends CoordinatorCommand
    final case class Done() extends CoordinatorCommand

    var numActors = 0

    /**
      * This actor runs the system. Currently, it also serves as the message hub. This will change in the future, as many messages do not need
      * to go through this intermediary. 
      * 
      * Another improvement is to scale the number of partsConstructor and PartValidators dynamically to increase parallelism
      *
      * @return 
      */
    def apply(): Behavior[CoordinatorCommand] = Behaviors.setup { context =>
        val boardValidator = context.spawn(BoardValidator(), "boardValidator")
        val validator = context.spawn(PartValidator(), "partValidator")

        def spawnCommand = Sudoko(context.self, boardValidator, validator)

        Behaviors.receiveMessage {
            case Start(name, rawBoard) => context.log.info("Spinning up new Sudoko!"); {
                val actor = context.spawn(spawnCommand, name)
                actor ! Commands.Run(rawBoard)
                numActors = numActors + 1
                Behaviors.same
            }
            case Done() => context.log.info("Spinning down a board"); numActors = numActors - 1; {
                if (numActors <= 0) Behaviors.stopped
                else Behaviors.same
            }
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
        // system ! Start(args(0), map.get(args(0)).get)

        map map { case (name, board) => system ! Start(name, board)}
    }
}

package fr.aphp.id.eds.requester.query

import org.apache.log4j.Logger
import play.api.libs.json.{JsArray, JsBoolean, JsResult, JsString, JsSuccess, JsValue, Json, Reads}
import org.json4s.jackson.Serialization

/** The QueryParser aimed at parsing the json query in string format to extract some info and map the json to an object structure */
object QueryParser {
  private val logger = Logger.getLogger(this.getClass)
  private val invalidJsonMessage = "Invalid json value"

  implicit lazy val idWithStringReads: Reads[IdWithString] =
    Json.reads[IdWithString]
  implicit lazy val idWithStringListReads: Reads[IdWithStringList] =
    Json.reads[IdWithStringList]
  implicit lazy val idWithIdBooleanReads: Reads[IdWithIdBoolean] =
    Json.reads[IdWithIdBoolean]
  implicit lazy val idWithDateBooleanReads: Reads[IdWithDateBoolean] =
    Json.reads[IdWithDateBoolean]

  /** Required to map a json to a "Either[String, List[Short]]" object */
  sealed trait DataValue
  case class DataValueString(s: String) extends DataValue
  case class DataValueShortList(iList: List[Short]) extends DataValue
  private implicit val dataValueReads: Reads[DataValue] = new Reads[DataValue] {
    override def reads(json: JsValue): JsResult[DataValue] = {
      json match {
        case JsString(s) => JsSuccess(DataValueString(s))
        case JsArray(s) =>
          JsSuccess(DataValueShortList(s.toList.map(x => x.asOpt[Short].get)))
        case _ => throw new Exception(invalidJsonMessage)
      }
    }
  }

  /** Required to map a json to a "Either[Boolean, List[Obj[Short, Boolean]]" object */
  sealed trait BoolValueId
  case class BoolValueIdOnly(s: Boolean) extends BoolValueId
  case class BoolValueIdList(iList: List[IdWithIdBoolean]) extends BoolValueId
  private implicit val boolValueIdReads: Reads[BoolValueId] =
    new Reads[BoolValueId] {
      override def reads(json: JsValue): JsResult[BoolValueId] = {
        json match {
          case JsBoolean(b) => JsSuccess(BoolValueIdOnly(b))
          case JsArray(s) =>
            println("BoolValueId", s.toList.toString())
            JsSuccess(BoolValueIdList(s.toList.map(x => x.asOpt[IdWithIdBoolean].get)))
          case _ => throw new Exception(invalidJsonMessage)
        }
      }
    }

  /** Required to map a json to a Either[Boolean, List[Obj[Short, String]] object */
  sealed trait BoolValueDate
  case class BoolValueDateOnly(s: Boolean) extends BoolValueDate
  case class BoolValueDateList(iList: List[IdWithDateBoolean]) extends BoolValueDate
  private implicit val boolValueDateReads: Reads[BoolValueDate] =
    new Reads[BoolValueDate] {
      override def reads(json: JsValue): JsResult[BoolValueDate] = {
        json match {
          case JsBoolean(b) => JsSuccess(BoolValueDateOnly(b))
          case JsArray(s) =>
            println("BoolValueDate", s.toList.toString())
            JsSuccess(BoolValueDateList(s.toList.map(x => x.asOpt[IdWithDateBoolean].get)))
          case _ => throw new Exception(invalidJsonMessage)
        }
      }
    }

  /** The GenericTemporalConstraint class is different from the TemporalConstraint class because Map(_id -> value) makes
    * more sense than a List[Map(_id: ..., value: ...)].
    * Parameters correspond to the spec that can be found here : https://gitlab.eds.aphp.fr/dev/query-server/suivi-projet/-/wikis/technical_spec_V1.2.1
    *
    * @param idList can be "all" or a list of short. It is the list of criteria id concerned by the constraint.
    * @param constraintType the type of temporal constraint.
    * @param occurrenceChoiceList list of couple (criterion id, type of occurrence choice).
    *                             Default type of occurrence choice is any.
    * @param timeRelationMinDuration minimum duration required between criteria of idList
    *                                (only usefull when 'constraintType=directChronologicalOrdering').
    * @param timeRelationMaxDuration maximum duration required between criteria of idList
    *                                (only usefull when 'constraintType=directChronologicalOrdering').
    * @param datePreferenceList list of couple (criterion id, date preference).
    *                           Date preference is a list of date to consider in case of null as the date of the ressource.
    *                           Default depends on the ressource.
    * @param filteredCriteriaIdList "all" or list of short. List of criteria id that will be filtered out by the constraint.
    * @param dateIsNotNullList boolean or list of couple (criterion id, boolean). Default is true.
    *                          filter out occurrence with null date or not.
    *  */
  case class GenericTemporalConstraint(idList: DataValue,
                                       constraintType: String,
                                       occurrenceChoiceList: Option[List[IdWithString]],
                                       timeRelationMinDuration: Option[TemporalConstraintDuration],
                                       timeRelationMaxDuration: Option[TemporalConstraintDuration],
                                       datePreferenceList: Option[List[IdWithStringList]],
                                       filteredCriteriaIdList: Option[DataValue],
                                       dateIsNotNullList: Option[BoolValueDate])

  /** Object that can handle all accepted json input.
    *
    * @param _type type of the json object (top level or group or basic ressource)
    * @param _id id of the group or basic ressource
    * @param version the version of the json schema (only for top level json obj)
    * @param request the nested object request (only for top level json obj)
    * @param isInclusive whether the basic ressource or groupe is an inclusive criterion or not
    * @param resourceType the SolR collection of a basic ressource
    * @param filterSolr the solr filter of a basic ressource
    * @param occurrence the required nbre and operator of occurrences for a basic ressource
    * @param patientAge the required min and/or max age of occurrences for a basic ressource
    * @param temporalConstraints list of GenericTemporalConstraint for group
    * @param criteria list of GenericQuery for group
    * @param nAmongMOptions specific options for nAmongM groups
    * @param sourcePopulation specific object with origin caresite and provider cohorts on which the cohort is built
    * @param nullAvailableFieldList list of string that can be null in the request.
    * @param dateRange required date min and/or max of occurrences for a basic ressource
    * */
  case class GenericQuery(_type: String,
                          _id: Option[Short],
                          isInclusive: Option[Boolean],
                          resourceType: Option[String],
                          filterSolr: Option[String],
                          occurrence: Option[Occurrence],
                          patientAge: Option[PatientAge],
                          temporalConstraints: Option[List[GenericTemporalConstraint]],
                          criteria: Option[List[GenericQuery]],
                          nAmongMOptions: Option[Occurrence],
                          version: Option[String],
                          sourcePopulation: Option[SourcePopulation],
                          request: Option[GenericQuery],
                          nullAvailableFieldList: Option[List[String]],
                          dateRangeList: Option[List[DateRange]],
                          encounterDateRange: Option[DateRange])

  /** Extracts the _id with at least one temporal constraint and their datePreference parameter
    * (required for naming the cache properly).
    * Extracts also the "request" object.
    * */
  def parse(cohortDefinitionSyntaxJsonString: String): (Request, Map[Short, CriterionTags]) = {
    import play.api.libs.json._
    import org.json4s._
    implicit val formats = Serialization.formats(NoTypeHints)

    implicit lazy val occurrenceReads = Json.reads[Occurrence]
    implicit lazy val patientAgeReads = Json.reads[PatientAge]
    implicit lazy val temporalConstraintDurationReads =
      Json.reads[TemporalConstraintDuration]
    implicit lazy val temporalConstraintReads =
      Json.reads[GenericTemporalConstraint]
    implicit lazy val sourcePopulationReads = Json.reads[SourcePopulation]
    implicit lazy val dateRange = Json.reads[DateRange]
    implicit lazy val queryRead = Json.reads[GenericQuery]
    val cohortRequestOption =
      Json.parse(cohortDefinitionSyntaxJsonString).validate[GenericQuery]
    val cohortRequest = cohortRequestOption.get
    val criterionTagsMap =
      CriterionTagsParser.getCriterionTagsMap(
        cohortRequest,
        Map[Short, CriterionTags]()
      )
    val request = specJson(cohortRequest).right.get
    if (logger.isDebugEnabled)
      logger.debug(s"Json parsed : request=${request}, criterionIdWithTcMap=${criterionTagsMap}")
    (request, criterionTagsMap)
  }

  protected def specJson(genericQuery: GenericQuery,
                         parentTemporalConstraint: Option[List[TemporalConstraint]] = Option.empty)
    : Either[BaseQuery, Request] = {

    def removeEmptyGroup(x: GenericQuery): Boolean = {
      !(!List("request", "basicResource").contains(x._type) &&
        !x.criteria
          .getOrElse(List[GenericQuery]())
          .exists(x => removeEmptyGroup(x)))
    }

    /** Convert a GenericTemporalConstraint object to a TemporalConstraint */
    def convertToTemporalConstraint(
        genericTemporalConstraint: GenericTemporalConstraint): TemporalConstraint = {
      val idList = genericTemporalConstraint.idList match {
        case DataValueString(s)        => Left(s)
        case DataValueShortList(iList) => Right(iList)
      }

      val filteredCriteriaIdList =
        if (genericTemporalConstraint.filteredCriteriaIdList.isDefined) {
          genericTemporalConstraint.filteredCriteriaIdList.get match {
            case DataValueString(s)        => Some(Left(s))
            case DataValueShortList(iList) => Some(Right(iList))
          }
        } else None

      val occurrenceChoiceList =
        if (genericTemporalConstraint.occurrenceChoiceList.isDefined) {
          var m = Map[Short, String]()
          genericTemporalConstraint.occurrenceChoiceList.get.foreach(x => m += (x.i -> x.value))
          Some(m)
        } else None

      val datePreferenceList =
        if (genericTemporalConstraint.datePreferenceList.isDefined) {
          var m = Map[Short, List[String]]()
          genericTemporalConstraint.datePreferenceList.get.foreach(x => m += (x.i -> x.value))
          Some(m)
        } else None

      val dateIsNotNullList =
        if (genericTemporalConstraint.dateIsNotNullList.isDefined) {
          val res = genericTemporalConstraint.dateIsNotNullList.get match {
            case BoolValueDateOnly(s) => Left(s)
            case BoolValueDateList(b) =>
              var m = Map[Short, Boolean]()
              b.foreach(x => m += (x.i -> x.value))
              Right(m)
          }
          Some(res)
        } else None

      TemporalConstraint(
        idList,
        genericTemporalConstraint.constraintType,
        occurrenceChoiceList,
        genericTemporalConstraint.timeRelationMinDuration,
        genericTemporalConstraint.timeRelationMaxDuration,
        datePreferenceList,
        dateIsNotNullList,
        filteredCriteriaIdList
      )
    }

    def loadBasicResource(genericQuery: GenericQuery): BasicResource = {
      BasicResource(
        _id = genericQuery._id.get,
        isInclusive = genericQuery.isInclusive.get,
        resourceType = genericQuery.resourceType.get,
        filter = genericQuery.filterSolr.get,
        occurrence = genericQuery.occurrence,
        patientAge = genericQuery.patientAge,
        nullAvailableFieldList = genericQuery.nullAvailableFieldList,
        dateRangeList = genericQuery.dateRangeList,
        encounterDateRange = genericQuery.encounterDateRange
      )
    }

    def loadGroupResource(genericQuery: GenericQuery): GroupResource = {
      // build the temporal constraints objects and split them between id related or 'all'
      val (tcForAll, tcWithIds): (Option[List[TemporalConstraint]],
                                  Option[List[TemporalConstraint]]) =
        if (genericQuery.temporalConstraints.isDefined) {
          val tcByType = genericQuery.temporalConstraints.get
            .map(x => convertToTemporalConstraint(x))
            .groupBy(tc => tc.idList.isRight)
          (tcByType.get(false), tcByType.get(true))
        } else (None, None)

      // feed the construction of sub criteria with tc with ids (in case they are related to them)
      val criterion = genericQuery.criteria
        .getOrElse(List[GenericQuery]())
        .filter(x => removeEmptyGroup(x))
        .map(x => specJson(x, tcWithIds).left.get)
      val criterionIds = criterion.map((c) => c.i)

      // filter the tc with ids (from parent and own) to match those who match sub criteria ids
      val groupTcWithIds: List[TemporalConstraint] =
        if (parentTemporalConstraint.isDefined || tcWithIds.isDefined) {
          (parentTemporalConstraint.getOrElse(List()) ++ tcWithIds.getOrElse(List())).filter(
            tc =>
              tc.idList.isRight && criterionIds.distinct
                .intersect(tc.idList.right.get.distinct)
                .size == tc.idList.right.get.distinct.size)
        } else {
          List()
        }

      // join with global tc
      val temporalConstraints = tcForAll.getOrElse(List()) ++ groupTcWithIds

      GroupResource(
        _type = genericQuery._type,
        _id = genericQuery._id.get,
        isInclusive = genericQuery.isInclusive.get,
        criteria = criterion,
        temporalConstraints = Some(temporalConstraints),
        nAmongMOptions = genericQuery.nAmongMOptions
      )
    }

    genericQuery._type match {
      case "request" =>
        val requestOption = genericQuery.request
        val resourceType = genericQuery.resourceType.getOrElse("Patient")
        if (requestOption.isEmpty) {
          Right(
            Request(sourcePopulation = genericQuery.sourcePopulation.get,
                    request = None,
                    resourceType = resourceType))
        } else {
          val request: Option[BaseQuery] = requestOption.get._type match {
            case "basicResource" =>
              Some(loadBasicResource(requestOption.get))
            case _ =>
              val group = loadGroupResource(requestOption.get)
              if (group.criteria.isEmpty) None else Some(group)
          }
          Right(
            Request(sourcePopulation = genericQuery.sourcePopulation.get,
                    request = request,
                    resourceType = resourceType))
        }
      case "basicResource" =>
        Left(loadBasicResource(genericQuery))
      case "andGroup" | "orGroup" | "nAmongM" =>
        Left(loadGroupResource(genericQuery))
    }
  }

}

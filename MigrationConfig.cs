using System.Collections.Generic;

namespace Logistics.DbMerger
{
    public static class MigrationConfig
    {
        /// <summary>
        /// Sentinel tenant ID used in DataSyncCheckpoint for global tables (no real tenant).
        /// Value 0 is safe because tenant IDs start from 1.
        /// </summary>
        public const int GlobalTableCheckpointSentinel = 0;

        /// <summary>
        /// Global tables (no TenantId) that need MERGE upsert instead of insert-only.
        /// Excludes Tenants (skipped via ABP strategy).
        /// </summary>
        public static readonly HashSet<string> GlobalTables = new(StringComparer.OrdinalIgnoreCase)
        {
            "Editions",
            "AllowableAbsence",
            "SubThreadType"
        };

        /// <summary>
        /// MERGE match key column for each global table.
        /// GUID PK tables match on PK directly; identity PK tables (Editions) match on a business column.
        /// </summary>
        public static readonly Dictionary<string, string> GlobalTableNaturalKeys = new(StringComparer.OrdinalIgnoreCase)
        {
            { "Editions", "Name" },
            { "AllowableAbsence", "AllowableAbsenceID" },
            { "SubThreadType", "SubThreadTypeID" }
        };

        // Define the explicit order of tables for migration (Tiers 1-8)
        public static readonly List<string> TableOrder = new List<string>
        {
            // 4.1 Tier 1 - Reference/Lookup Tables
            "Tenants",
            "Languages",
            "Editions",
            "Features",
            "Status",
            "ReasonType",
            "Reason",
            "BudgetType",
            "DiscussionType",
            "OutcomeType",
            "ThreadType",
            "SubThreadType",
            "ConsultationType",
            "TaskType",
            "WorkType",
            "VolumeType",
            "UnitType",
            "PalletType",
            "TemplateType",
            "RateType",
            "LeaveType",
            "PublicHolidayGroup",
            "PublicHolidayGroupDay",
            "RDOGroup",
            "RDOGroupDetail",
            "Category",
            "ProvidedToType",
            "RoleType",
            "AllowableAbsence",

            // 4.2 Tier 2 - Core Entity Tables
            "Grade",
            "Position",
            "EmploymentType",
            "EmploymentAgency",
            "ManagerGroup",
            "Shift",
            "RosterPattern",
            "RosterPatternDetail",
            "RosterPatternDetailItem",
            "EBA",
            "EBAGradeRate",
            "Function",
            "Area",
            "AreaClockNum",
            "Chamber",
            "ChamberFloorCapacity",
            "Qualification",
            "QualificationMatrix",
            "CoverageGroup",
            "FilterGroup",
            "DayDefinition",
            "CommentTemplate",
            "DoomDashboardTemplate",
            "Agency",
            "TenantTimeZone",

            // 4.3 Tier 3 - Primary Operational Tables
            "Contact",
            "ContactRDOGroup",
            "AgencyContact",
            "Users",
            "UserAccounts",
            "UserRoles",
            "UserClaims",
            "UserLogins",
            "UserLoginAttempts",
            "UserNotifications",
            "UserOrganizationUnits",
            "OrganizationUnits",
            "ManagerGroupUser",
            "ContactFilterGroups",
            "ContactQualification",
            "ContactQualificationAreaTask",
            "ContactQualificationSuspension",
            "ContactEBA",
            "ContactPerformance",
            "ContactLMPerformance",
            "ContactNotification",

            // 4.4 Tier 4 - Transactional Tables
            "Task",
            "LeaveRequest",
            "LeaveBalance",
            "LeaveBlackOut",
            "Attachment",
            "ContactComments",
            "ContactDiscussion",
            "TemplateSchedule",
            "TemplateScheduleDetail",
            "ContactTemplateSchedule",
            "ContactTemplateScheduleOverride",
            "Budget",
            "FloorCapacity",
            "CommercialShift",
            "CommercialVolume",
            "LoadManagementCalc",
            "AgencyInvoice",
            "ActualActivityConfiguration",
            "OffTaskMinutesByTask",

            // 4.5 Tier 5 - High-Volume Operational Tables
            "TimebandHeader",
            "Timeband",
            "TimebandDetail",
            "TimebandDetailBreakdown",
            "TimebandShiftBid",
            "PlannedTimeband",
            "PlannedTimebandDetail",
            "ForecastTimeband",
            "ForecastTimebandDetail",
            "LastRosteredTimeband",
            "ClockEvent",
            "IndirectClockEvent", // ADC Name
            "IndirectClockEvents", // MDC Name (Explicitly mapped, but listed for completeness)
            "ClockingTransactionSyncTempDatas",
            "ClockIntegrationSyncJobHistories",
            "IndirectClockingTransactionSyncTempDatas",
            "KronosEmployeeTemps",
            "VolumeBlockData",
            "VolumeDetail",
            "VolumeDetailAudit",
            "VolumeDetailMaster",
            "VolumeTaskConversion",
            "BulkVolumeDetail",

            // 4.6 Tier 6 - Secondary/Support Tables
            "ShiftBid",
            "ShiftBidDetail",
            "ShiftDailyWeeklyLimit",
            "ShiftMoveReason",
            "ShiftScheduleTemplate",
            "ShiftScheduleTemplateBreak",
            "PlannedShiftBidOffers",
            "ContactShiftBid",
            "ReturnToWorkHeader",
            "ReturntoWorkPlan",
            "ReturnToWorkActivity",
            "ReturnToWorkConsultation",
            "ReturntoWorkGoal",
            "ReturnToWorkHeaderGoal",
            "ReturntoWorkPlanRestriction",
            "ReturnToWorkPlanDistribution",
            "TrainingPlan",
            "TrainingPlanInstance",
            "TrainingPlanQualification",
            "TrainingPlanInstanceQualification",
            "TrainingReason",
            "TrainingTask",
            "ContactTrainingPlanInstance",

            // 4.7 Tier 7 - Rules & Audit Tables
            "ruletype",
            "ruleset",
            "Rule",
            "rulesetrule",
            "rulecalculation",
            "rulecalculationdependant",
            "ruleprocessingqueue",
            "ProcessedRule",
            "AuditLogs",
            "EntityAuditLog",
            "BackgroundJobs",

            // 4.8 Tier 8 - Notifications & Settings
            "LanguageTexts",
            "AppBinaryObjects",
            "AppChatMessages",
            "AppFriendships",
            "Setting",
            "Settings",
            "IntegrationSettings",
            "IntegrationException",
            "ExternalIntegrationQueue",
            "Notifications",
            "NotificationSubscriptions",
            "NotificationFilterTemplate",
            "HomeNotification",
            "HomeNotificationLog",
            "TenantNotifications",
            "ReportNotifications",
            "SftpReportHistory",
            "TAImportRecords",
            "GlobalFilter",
            "Permissions",
            "Roles",
            "LastUpdateBiarri",
            "DespatchTMS",
            "AbsenteeismRate",
            "TeamMemberPerformance",
            "WrongFunctionLog",
            "WrongFunctionTaskControl",
            "Numbers",
            "bulkreconcillation",
            "volumes"
        };

        /// <summary>
        /// Explicit tier grouping (1-8) for data sync by tier.
        /// Keys are tier numbers; values are table names belonging to that tier.
        /// </summary>
        public static readonly Dictionary<int, List<string>> TierTables = new Dictionary<int, List<string>>
        {
            {
                1, new List<string>
                {
                    "Tenants",
                    "Languages",
                    "Editions",
                    "Features",
                    "Status",
                    "ReasonType",
                    "Reason",
                    "BudgetType",
                    "DiscussionType",
                    "OutcomeType",
                    "ThreadType",
                    "SubThreadType",
                    "ConsultationType",
                    "TaskType",
                    "WorkType",
                    "VolumeType",
                    "UnitType",
                    "PalletType",
                    "TemplateType",
                    "RateType",
                    "LeaveType",
                    "PublicHolidayGroup",
                    "PublicHolidayGroupDay",
                    "RDOGroup",
                    "RDOGroupDetail",
                    "Category",
                    "ProvidedToType",
                    "RoleType",
                    "AllowableAbsence",
                }
            },
            {
                2, new List<string>
                {
                    "Grade",
                    "Position",
                    "EmploymentType",
                    "EmploymentAgency",
                    "ManagerGroup",
                    "Shift",
                    "RosterPattern",
                    "RosterPatternDetail",
                    "RosterPatternDetailItem",
                    "EBA",
                    "EBAGradeRate",
                    "Function",
                    "Area",
                    "AreaClockNum",
                    "Chamber",
                    "ChamberFloorCapacity",
                    "Qualification",
                    "QualificationMatrix",
                    "CoverageGroup",
                    "FilterGroup",
                    "DayDefinition",
                    "CommentTemplate",
                    "DoomDashboardTemplate",
                    "Agency",
                    "TenantTimeZone",
                }
            },
            {
                3, new List<string>
                {
                    "Contact",
                    "ContactRDOGroup",
                    "AgencyContact",
                    "Users",
                    "UserAccounts",
                    "UserRoles",
                    "UserClaims",
                    "UserLogins",
                    "UserLoginAttempts",
                    "UserNotifications",
                    "UserOrganizationUnits",
                    "OrganizationUnits",
                    "ManagerGroupUser",
                    "ContactFilterGroups",
                    "ContactQualification",
                    "ContactQualificationAreaTask",
                    "ContactQualificationSuspension",
                    "ContactEBA",
                    "ContactPerformance",
                    "ContactLMPerformance",
                    "ContactNotification",
                }
            },
            {
                4, new List<string>
                {
                    "Task",
                    "LeaveRequest",
                    "LeaveBalance",
                    "LeaveBlackOut",
                    "Attachment",
                    "ContactComments",
                    "ContactDiscussion",
                    "TemplateSchedule",
                    "TemplateScheduleDetail",
                    "ContactTemplateSchedule",
                    "ContactTemplateScheduleOverride",
                    "Budget",
                    "FloorCapacity",
                    "CommercialShift",
                    "CommercialVolume",
                    "LoadManagementCalc",
                    "AgencyInvoice",
                    "ActualActivityConfiguration",
                    "OffTaskMinutesByTask",
                }
            },
            {
                5, new List<string>
                {
                    "TimebandHeader",
                    "Timeband",
                    "TimebandDetail",
                    "TimebandDetailBreakdown",
                    "TimebandShiftBid",
                    "PlannedTimeband",
                    "PlannedTimebandDetail",
                    "ForecastTimeband",
                    "ForecastTimebandDetail",
                    "LastRosteredTimeband",
                    "ClockEvent",
                    "IndirectClockEvent",
                    "IndirectClockEvents",
                    "ClockingTransactionSyncTempDatas",
                    "ClockIntegrationSyncJobHistories",
                    "IndirectClockingTransactionSyncTempDatas",
                    "KronosEmployeeTemps",
                    "VolumeBlockData",
                    "VolumeDetail",
                    "VolumeDetailAudit",
                    "VolumeDetailMaster",
                    "VolumeTaskConversion",
                    "BulkVolumeDetail",
                }
            },
            {
                6, new List<string>
                {
                    "ShiftBid",
                    "ShiftBidDetail",
                    "ShiftDailyWeeklyLimit",
                    "ShiftMoveReason",
                    "ShiftScheduleTemplate",
                    "ShiftScheduleTemplateBreak",
                    "PlannedShiftBidOffers",
                    "ContactShiftBid",
                    "ReturnToWorkHeader",
                    "ReturntoWorkPlan",
                    "ReturnToWorkActivity",
                    "ReturnToWorkConsultation",
                    "ReturntoWorkGoal",
                    "ReturnToWorkHeaderGoal",
                    "ReturntoWorkPlanRestriction",
                    "ReturnToWorkPlanDistribution",
                    "TrainingPlan",
                    "TrainingPlanInstance",
                    "TrainingPlanQualification",
                    "TrainingPlanInstanceQualification",
                    "TrainingReason",
                    "TrainingTask",
                    "ContactTrainingPlanInstance",
                }
            },
            {
                7, new List<string>
                {
                    "ruletype",
                    "ruleset",
                    "Rule",
                    "rulesetrule",
                    "rulecalculation",
                    "rulecalculationdependant",
                    "ruleprocessingqueue",
                    "ProcessedRule",
                    "AuditLogs",
                    "EntityAuditLog",
                    "BackgroundJobs",
                }
            },
            {
                8, new List<string>
                {
                    "LanguageTexts",
                    "AppBinaryObjects",
                    "AppChatMessages",
                    "AppFriendships",
                    "Setting",
                    "Settings",
                    "IntegrationSettings",
                    "IntegrationException",
                    "ExternalIntegrationQueue",
                    "Notifications",
                    "NotificationSubscriptions",
                    "NotificationFilterTemplate",
                    "HomeNotification",
                    "HomeNotificationLog",
                    "TenantNotifications",
                    "ReportNotifications",
                    "SftpReportHistory",
                    "TAImportRecords",
                    "GlobalFilter",
                    "Permissions",
                    "Roles",
                    "LastUpdateBiarri",
                    "DespatchTMS",
                    "AbsenteeismRate",
                    "TeamMemberPerformance",
                    "WrongFunctionLog",
                    "WrongFunctionTaskControl",
                    "Numbers",
                    "bulkreconcillation",
                    "volumes",
                }
            },
        };
    }
}

package dpfm_api_output_formatter

type SDC struct {
	ConnectionKey       string      `json:"connection_key"`
	Result              bool        `json:"result"`
	RedisKey            string      `json:"redis_key"`
	Filepath            string      `json:"filepath"`
	APIStatusCode       int         `json:"api_status_code"`
	RuntimeSessionID    string      `json:"runtime_session_id"`
	BusinessPartnerID   *int        `json:"business_partner"`
	ServiceLabel        string      `json:"service_label"`
	APIType             string      `json:"api_type"`
	Message             interface{} `json:"message"`
	APISchema           string      `json:"api_schema"`
	Accepter            []string    `json:"accepter"`
	Deleted             bool        `json:"deleted"`
	SQLUpdateResult     *bool       `json:"sql_update_result"`
	SQLUpdateError      string      `json:"sql_update_error"`
	SubfuncResult       *bool       `json:"subfunc_result"`
	SubfuncError        string      `json:"subfunc_error"`
	ExconfResult        *bool       `json:"exconf_result"`
	ExconfError         string      `json:"exconf_error"`
	APIProcessingResult *bool       `json:"api_processing_result"`
	APIProcessingError  string      `json:"api_processing_error"`
}

type Message struct {
	Batch         *[]Batch      `json:"Batch"`
}

type Batch struct {
	Product				string  `json:"Product"`
	BusinessPartner		int		`json:"BusinessPartner"`
	Plant				string	`json:"Plant"`
	Batch				string	`json:"Batch"`
	ValidityStartDate	string	`json:"ValidityStartDate"`
	ValidityStartTime	string	`json:"ValidityStartTime"`
	ValidityEndDate		string	`json:"ValidityEndDate"`
	ValidityEndTime		string	`json:"ValidityEndTime"`
	ManufactureDate		*string	`json:"ManufactureDate"`
	CreationDate         string  `json:"CreationDate"`
	CreationTime         string  `json:"CreationTime"`
	LastChangeDate       string  `json:"LastChangeDate"`
	LastChangeTime       string  `json:"LastChangeTime"`	
	IsMarkedForDeletion  *bool   `json:"IsMarkedForDeletion"`
}

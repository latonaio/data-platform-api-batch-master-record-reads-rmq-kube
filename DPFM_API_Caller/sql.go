package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-batch-master-record-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-batch-master-record-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var batch *dpfm_api_output_formatter.Batch
	for _, fn := range accepter {
		switch fn {
		case "Batch":
			func() {
				batch = c.Batch(mtx, input, output, errs, log)
			}()
		case "Batches":
			func() {
				batch = c.Batches(mtx, input, output, errs, log)
			}()
		default:
		}
	}

	data := &dpfm_api_output_formatter.Message{
		Batch:         batch,
	}

	return data
}

func (c *DPFMAPICaller) Batch(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Batch {
	where := fmt.Sprintf("WHERE batch.Product = \"%s\"", input.Batch.Product)
	where := fmt.Sprintf("WHERE batch.BusinessPartner = %d ", input.Batch.BusinessPartner)
	where := fmt.Sprintf("WHERE batch.Plant = \"%s\"", input.Batch.Plant)
	where := fmt.Sprintf("WHERE batch.Batch = %d ", input.Batch.Batch)

	if input.Batch.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND batch.IsMarkedForDeletion = %v", where, *input.Batch.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_batch-master-record_batch_data AS batch
		` + where + ` ORDER BY batch.IsMarkedForDeletion ASC, batch.BatchID DESC;`,
	)

	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToBatch(input, rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return &((*data)[0])
}

func (c *DPFMAPICaller) Batches(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Batch {

	where := "WHERE 1 = 1"
	
	if input.Batch.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND batch-master-record.IsMarkedForDeletion = %v", where, *input.Batch.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_batch-master-record_batch_data AS batch
		` + where + ` ORDER BY batch-master-record.IsMarkedForDeletion ASC, batch.Product DESC, batch.BusinessPartner DESC, batch.Plant DESC, batch.Batch DESC;`,
	)

	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToBatch(input, rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return &((*data)[0])
}

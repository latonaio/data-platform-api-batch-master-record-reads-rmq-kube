package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-batch-master-record-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-batch-master-record-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"strings"
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
	var batch *[]dpfm_api_output_formatter.Batch

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
		Batch: batch,
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
	where = fmt.Sprintf("%s\nAND batch.BusinessPartner =  \"%s\"", where, input.Batch.Product)
	where = fmt.Sprintf("%s\nAND batch.Plant =  \"%s\"", where, input.Batch.Plant)
	where = fmt.Sprintf("%s\nAND batch.Batch =  \"%s\"", where, input.Batch.Batch)

	if input.Batch.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND batch.IsMarkedForDeletion = %v", where, *input.Batch.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_batch_master_record_batch_data AS batch
		` + where + ` ORDER BY batch.IsMarkedForDeletion ASC, batch.Batch DESC;`,
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

	return data
}

func (c *DPFMAPICaller) Batches(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Batch {
	var args []interface{}

	cnt := 0

	for _, vBatch := range input.Batches {
		product := vBatch.Product
		businessPartner := vBatch.BusinessPartner
		plant := vBatch.Plant
		batch := vBatch.Batch

		args = append(args, product, businessPartner, plant, batch)
		cnt++
	}

	repeat := strings.Repeat("(?,?,?,?),", cnt-1) + "(?,?,?,?)"

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_batch_master_record_batch_data
		WHERE (Product, BusinessPartner, Plant, Batch) IN ( `+repeat+` )`, args...,
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

	return data
}

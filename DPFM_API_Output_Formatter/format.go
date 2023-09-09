package dpfm_api_output_formatter

import (
	"data-platform-api-batch-master-record-reads-rmq-kube/DPFM_API_Caller/requests"
	api_input_reader "data-platform-api-batch-master-record-reads-rmq-kube/DPFM_API_Input_Reader"
	"database/sql"
	"fmt"
)

func ConvertToBatch(sdc *api_input_reader.SDC, rows *sql.Rows) (*[]Batch, error) {
	defer rows.Close()
	batches := make([]Batch, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Batch{}

		err := rows.Scan(
			&pm.Product,
			&pm.BusinessPartner,
			&pm.Plant,
			&pm.Batch,
			&pm.ValidityStartDate,
			&pm.ValidityStartTime,
			&pm.ValidityEndDate,
			&pm.ValidityEndTime,
			&pm.ManufactureDate,
			&pm.CreationDate,
			&pm.CreationTime,
			&pm.LastChangeDate,
			&pm.LastChangeTime,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &batches, err
		}

		data := pm

		batches = append(batches, Batch{
			Product:             data.Product,
			BusinessPartner:     data.BusinessPartner,
			Plant:               data.Plant,
			Batch:               data.Batch,
			ValidityStartDate:   data.ValidityStartDate,
			ValidityStartTime:   data.ValidityStartTime,
			ValidityEndDate:     data.ValidityEndDate,
			ValidityEndTime:     data.ValidityEndTime,
			ManufactureDate:     data.ManufactureDate,
			CreationDate:        data.CreationDate,
			CreationTime:        data.CreationTime,
			LastChangeDate:      data.LastChangeDate,
			LastChangeTime:      data.LastChangeTime,
			IsMarkedForDeletion: data.IsMarkedForDeletion,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &batches, nil
	}

	return &batches, nil
}

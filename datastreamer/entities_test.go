package datastreamer_test

/*
func TestEntityToString(t *testing.T) {
	l2Transaction := db.L2Transaction{
		BatchNumber:                 1,                     // 8 bytes
		EffectiveGasPricePercentage: 128,                   // 1 byte
		IsValid:                     1,                     // 1 byte
		EncodedLength:               5,                     // 4 bytes
		Encoded:                     []byte{1, 2, 3, 4, 5}, // 5 bytes
	}

	entityDef := datastreamer.EntityDefinition{
		Name:       "L2Transaction",
		StreamType: datastreamer.StreamType(db.StreamTypeSequencer),
		Definition: reflect.TypeOf(db.L2Transaction{}),
	}

	stringValue := entityDef.ToString(l2Transaction.Encode())
	expected := "BatchNumber: 1, EffectiveGasPricePercentage: 128, IsValid: 1, EncodedLength: 5, Encoded: [slice]"

	assert.Equal(t, expected, stringValue)

}
*/

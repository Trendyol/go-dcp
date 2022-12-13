package model

import "encoding/json"

type Identity struct {
	Ip   string
	Name string
}

func (k *Identity) String() string {
	str, err := json.Marshal(k)

	if err != nil {
		panic(err)
	}

	return string(str)
}

func (k *Identity) LoadFromString(str string) {
	err := json.Unmarshal([]byte(str), k)

	if err != nil {
		panic(err)
	}
}

func (k *Identity) Equal(other *Identity) bool {
	return k.Ip == other.Ip && k.Name == other.Name
}

func NewIdentityFromStr(str string) *Identity {
	identity := &Identity{}
	identity.LoadFromString(str)

	return identity
}

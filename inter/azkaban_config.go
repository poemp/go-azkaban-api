package inter

//this is to azkaban config
const (
	Url      = "http://39.105.240.237:8083/"
	UserName = "azkaban"
	Password = "azkaban"
)

//this is azkaban config entity
type AzkabanConfig struct {
	Url      string
	UserName string
	Password string
}

var DefaultAzkabanConfig = func() AzkabanConfig {
	return AzkabanConfig{
		Url:      Url,
		UserName: UserName,
		Password: Password,
	}
}

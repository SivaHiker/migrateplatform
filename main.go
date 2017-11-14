package main

import (
	"time"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"strconv"
	"os"
	"bufio"
	"encoding/csv"
	"strings"
	"io"
	"database/sql"
)

func main() {
	var count int64
	file, err := os.Open("/home/siva/LatestAppOpenUsers_20170512_to_20171107.txt")
	defer file.Close()

	if err != nil {
		println(err)
	}

	dbConn := getDBConnection()
	dbConn.SetMaxOpenConns(10000)

	defer dbConn.Close()
	err = dbConn.Ping()
	if err != nil {
		fmt.Println(err.Error())
	}
	// Start reading from the file with a reader.
	reader := bufio.NewReader(file)

	outputfile1, err := os.Create("text1.txt")
	if(err!=nil){
		fmt.Println("Not able to create a file")
	}
	defer outputfile1.Close()

	outputfile2, err := os.Create("text2.txt")
	if(err!=nil){
		fmt.Println("Not able to create a file")
	}
	defer outputfile2.Close()

	csvfile1, err := os.Create("result1.csv")
	if(err!=nil){
		fmt.Println("Not able to create a csv file")
	}

	writer1 := csv.NewWriter(csvfile1)
	defer writer1.Flush()
	defer csvfile1.Close()


	csvfile2, err := os.Create("result2.csv")
	if(err!=nil){
		fmt.Println("Not able to create a csv file")
	}

	writer2 := csv.NewWriter(csvfile2)
	defer writer2.Flush()
	defer csvfile2.Close()

	limiter := time.Tick(time.Nanosecond * 1000000)

	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		var user platformUser
		//var userdetails platformUserDetails
		//fmt.Printf(" > Read %d characters\n", len(line))
		//fmt.Println(line)
		uservalues := strings.Split(line,"+")
		uid := uservalues[0]


		uid="WcKAaVIchw737usm"
		fmt.Println("select * from platform_user where  hike_uid=\""+uid+"\"")
		<-limiter
		rows1,err := dbConn.Query("select * from platform_user where  hike_uid=\""+uid+"\"")
		if(err!=nil){
			fmt.Println("Not able to query the hike uid in the DB -->",uid,err)
		}
		//fmt.Println("select * from platform_user_details where  hike_uid=\""+uid+"\"")
		//rows2,err := dbConn.Query("select * from platform_user_details where hike_uid=\""+uid+"\"")
		//if(err!=nil){
		//	fmt.Println("Not able to query the hike uid in the DB -->",uid,err)
		//}

		if rows1.Next() {
			fmt.Println("Entered")
			err := rows1.Scan(&user.ID,&user.HikeUID, &user.PlatformUID, &user.PlatformToken, &user.Msisdn,
				&user.HikeToken,&user.CreateTime,&user.UpdateTs, &user.Status)
			fmt.Println("Entered2")
			if(err!=nil) {
				fmt.Println(err.Error())
			}
		}
		//if(rows2.Next()) {
		//	err := rows2.Scan(&userdetails.ID,&userdetails.HikeUID, &userdetails.Msisdn, &userdetails.Name,
		//		&userdetails.Gender,
		//		&userdetails.Circle, &userdetails.CreateTime, &userdetails.UpdateTime)
		//	fmt.Println(err)
		//}
		//userValues ={userd.Token,userd.Msisdn,userd.UID,userd.AppVersion,userd.DeviceKey,userd.DevID,
		//userd.RegTime,userd.DevToken,userd.DevTokenUpdateTs,userd.DevVersion,userd.DevType,userd.Os,
		//userd.OsVersion,userd.UpgradeTime,userd.LastActivityTime,userd.AttributeBits,userd.Sound,userd.EndTime,
		//userd.OriginalAppVersion,userd.Operator,userd.Resolution,userd.Circle,userd.Pdm}

		//outputfile.WriteString(ToString(userd.Token)+"::+"+ToIntegerVal(userd.Msisdn)+"::"+ToString(userd.
		//	Sound)+"::"+ToIntegerVal(userd.UpgradeTime)+"\n")

		fmt.Println("Entered3")
		msisdnReqd := user.Msisdn
		if strings.HasPrefix(msisdnReqd,"9") {
			msisdnReqd=strings.Replace(msisdnReqd,"9","1",1)
		} else if (strings.HasPrefix(msisdnReqd,"9")) {
			msisdnReqd=strings.Replace(msisdnReqd,"8","2",1)
		} else if (strings.HasPrefix(msisdnReqd,"9")) {
			msisdnReqd=strings.Replace(msisdnReqd,"7","3",1)
		} else {
			continue
		}
		fmt.Println("The Msisdn got ",msisdnReqd)
		//msisdnReqd2 := userdetails.Msisdn
		//if strings.HasPrefix(msisdnReqd,"9") {
		//	msisdnReqd2=strings.Replace(msisdnReqd2,"9","1",1)
		//} else if (strings.HasPrefix(msisdnReqd2,"9")) {
		//	msisdnReqd2=strings.Replace(msisdnReqd2,"8","2",1)
		//} else if (strings.HasPrefix(msisdnReqd2,"9")) {
		//	msisdnReqd2=strings.Replace(msisdnReqd2,"7","3",1)
		//} else {
		//	continue
		//}
		count++
		outputfile1.WriteString(ToIntegerVal(count)+"::"+user.HikeUID+"::"+user.PlatformUID+"::"+user.
			PlatformToken+"::+"+msisdnReqd+"::"+user.HikeToken+"::"+user.CreateTime.String()+"::"+user.UpdateTs.String()+
				"::"+ToString(user.Status)+"\n")

		records1 := [][]string{
			{ToIntegerVal(count),user.HikeUID,user.PlatformUID,user.PlatformToken,"+"+msisdnReqd,user.HikeToken,
			user.CreateTime.String(),user.UpdateTs.String(), ToString(user.Status)},
		}

		//outputfile2.WriteString(ToIntegerVal(count)+"::"+userdetails.HikeUID+"::"+"+"+msisdnReqd+"::"+ToString(userdetails.
		//	Name)+"::"+ ToString(userdetails.Gender)+"::"+ToString(userdetails.Circle)+"::"+
		//		"::"+userdetails.CreateTime.String()+"::"+userdetails.UpdateTime.String()+"\n")
		//
		//records2 := [][]string{
		//	{ToIntegerVal(count),userdetails.HikeUID,"+"+msisdnReqd2,ToString(userdetails.Name),
		//	ToString(userdetails.Gender),ToString(userdetails.Circle), userdetails.CreateTime.String(),userdetails.UpdateTime.String()},
		//	}

		for _, value := range records1 {
			err := writer1.Write(value)
			if(err!=nil){
				fmt.Println(err.Error())
				fmt.Println("Not able to write the records into csv file")
			}
		}

		//for _, value := range records2 {
		//	err := writer2.Write(value)
		//	if(err!=nil){
		//		fmt.Println(err.Error())
		//		fmt.Println("Not able to write the records into csv file")
		//	}
		//}

		rows1.Close()
		//rows2.Close()
	}

	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}

}

func getDBConnection() *sql.DB{

	db, err := sql.Open("mysql", "platform:p1@tf0rmD1st@tcp(10.15.8.4:3306)/usersdb?parseTime=true")
	if(err!=nil){
		fmt.Println(err)
	}
	return db
}

func ToNullString(s string) sql.NullString {
	return sql.NullString{String : s, Valid : s != ""}
}

func ToIntegerVal(i int64) string {
	var valueInt string
	valueInt = strconv.FormatInt(int64(i), 10)
	return valueInt
}

func ToStringFromInt(i int) string {
	var valueInt string
	valueInt = strconv.Itoa(i)
	return valueInt
}

func ToString(s sql.NullString) string {
	var valInString string
	if(s.Valid) {
		valInString = s.String
		fmt.Println(valInString)
	} else {
		valInString = "NULL"
		fmt.Println(valInString)
	}
	return valInString
}

type platformUser struct {
	CreateTime    time.Time  `json:"create_time"`
	HikeToken     string `json:"hike_token"`
	HikeUID       string `json:"hike_uid"`
	ID            int64    `json:"id"`
	Msisdn        string `json:"msisdn"`
	PlatformToken string `json:"platform_token"`
	PlatformUID   string `json:"platform_uid"`
	Status        sql.NullString `json:"status"`
	UpdateTs      time.Time `json:"update_ts"`
}

type platformUserDetails struct {
	Circle     sql.NullString `json:"circle"`
	CreateTime time.Time `json:"create_time"`
	Gender     sql.NullString `json:"gender"`
	HikeUID    string `json:"hike_uid"`
	ID         int64    `json:"id"`
	Msisdn     string `json:"msisdn"`
	Name       sql.NullString `json:"name"`
	UpdateTime time.Time `json:"update_time"`
}
package ru.ilk.creditcard;//package ru.ilk.creditcard;

public class Creditcard {
    public static class TransactionKafka {
        private static String cc_num = "cc_num";
        private static String first = "first";
        private static String last = "last";
        private static String trans_num = "trans_num";
        private static String trans_date = "trans_date";
        private static String trans_time = "trans_time";
        private static String unix_time = "unix_time";
        private static String category = "category";
        private static String merchant = "merchant";
        private static String amt = "amt";
        private static String merch_lat = "merch_lat";
        private static String merch_long = "merch_long";
        private static String distance = "distance";
        private static String age = "age";
        private static String is_fraud = "is_fraud";
        private static String kafka_partition = "partition";
        private static String kafka_offset = "offset";

        public static String getCc_num() {
            return cc_num;
        }
        public void setCc_num(String cc_num) {
            this.cc_num = cc_num;
        }

        public static String getFirst() {
            return first;
        }
        public void setFirst(String cc_num) {
            this.first = first;
        }

        public static String getLast() {
            return last;
        }
        public void setLast(String last) {
            this.last = last;
        }

        public static String getTrans_num() {
            return trans_num;
        }
        public void setTrans_num(String trans_num) {
            this.trans_num = trans_num;
        }

        public static String getTrans_date() {
            return trans_date;
        }
        public void setTrans_date(String trans_date) {
            this.trans_date = trans_date;
        }

        public static String getTrans_time() {
            return trans_time;
        }
        public void setTrans_time(String trans_time) {
            this.trans_time = trans_time;
        }

        public static String getUnix_time() {
            return unix_time;
        }
        public void setUnix_time(String unix_time) {
            this.unix_time = unix_time;
        }

        public static String getCategory() {
            return category;
        }
        public void setCategory(String category) {
            this.category = category;
        }

        public static String getMerchant() {
            return merchant;
        }
        public void setMerchant(String merchant) {
            this.merchant = merchant;
        }

        public static String getAmt() {
            return amt;
        }
        public void setAmt(String amt) {
            this.amt = amt;
        }

        public static String getMerch_lat() {
            return merch_lat;
        }
        public void setMerch_lat(String merch_lat) {
            this.merch_lat = merch_lat;
        }

        public static String getMerch_long() {
            return merch_long;
        }
        public void setMerch_long(String merch_long) {
            this.merch_long = merch_long;
        }

        public static String getDistance() {
            return distance;
        }
        public void setDistance(String distance) {
            this.distance = distance;
        }

        public static String getAge() {
            return age;
        }
        public void setAge(String age) {
            this.age = age;
        }

        public static String getIs_fraud() {
            return is_fraud;
        }
        public void setIs_fraud(String is_fraud) {
            this.is_fraud = is_fraud;
        }

        public static String getKafka_partition() {
            return kafka_partition;
        }
        public void setKafka_partition(String kafka_partition) {
            this.kafka_partition = kafka_partition;
        }

        public static String getKafka_offset() {
            return kafka_offset;
        }
        public void setKafka_offset(String kafka_offset) {
            this.kafka_offset = kafka_offset;
        }
    }

    public static class Customer {
        private static String cc_num = "cc_num";
        private static String first = "first";
        private static String last = "last";
        private static String gender = "gender";
        private static String street = "street";
        private static String city = "city";
        private static String state = "state";
        private static String zip = "zip";
        private static String lat = "lat";
        private static String long_field = "long";
        private static String job = "job";
        private static String dob = "dob";

        public static String getCc_num() {
            return cc_num;
        }
        public void setCc_num(String cc_num) {
            this.cc_num = cc_num;
        }

        public static String getFirst() {
            return first;
        }
        public void setFirst(String first) {
            this.first = first;
        }

        public static String getLast() {
            return last;
        }
        public void setLast(String last) {
            this.last = last;
        }

        public static String getGender() {
            return gender;
        }
        public void setGender(String gender) {
            this.gender = gender;
        }

        public static String getStreet() {
            return street;
        }
        public void setStreet(String street) {
            this.street = street;
        }

        public static String getCity() {
            return city;
        }
        public void setCity(String city) {
            this.city = city;
        }

        public static String getState() {
            return state;
        }
        public void setState(String state) {
            this.state = state;
        }

        public static String getZip() {
            return zip;
        }
        public void setZip(String zip) {
            this.zip = zip;
        }

        public static String getLat() {
            return lat;
        }
        public void setLat(String lat) {
            this.lat = lat;
        }

        public static String getLong() {
            return long_field;
        }
        public void setLong(String long_field) {
            this.long_field = long_field;
        }

        public static String getJob() {
            return job;
        }
        public void setJob(String job) {
            this.job = job;
        }

        public static String getDob() {
            return dob;
        }
        public void setDob(String dob) {
            this.dob = dob;
        }


    }

    public class TransactionCassandra extends TransactionKafka {
    }
}

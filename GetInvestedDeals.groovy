import groovy.sql.Sql

def dbUrl = 'jdbc:postgresql://localhost:5432/test'
def dbUser = 'sukram-code'
def dbPassword = 'test'
def dbDriver = 'org.postgresql.Driver'

def conn = Sql.newInstance(dbUrl, dbUser, dbPassword, dbDriver)

println 'Connected'

def sql = "SELECT * FROM subscription s JOIN buy_order bo ON bo.subscription_id = s.subscription_id WHERE s.status = 'ACTIVE'"
def dealIds = [:]

conn.query(sql) { resultSet ->
  while (resultSet.next()) {
    def dealId = resultSet.getString('deal_id')
    if (dealId != null) {
      dealIds[dealId] = dealId
    }
  }
}

def joinedDeals = dealIds.keySet().collect{":$it"}.join(',')
def sql2 = "SELECT order_id, status, deal_id FROM buy_order bo WHERE bo.deal_id IN ($joinedDeals)"

println "Executing: $sql2"

conn.eachRow(sql2, dealIds) { row ->
  println row
}

conn.close()

println 'Done.'

library(ggplot2)
library(scales)
library(plyr)
library(grid)


label_wrapper <- function(variable, value, ...) {
  llply(as.character(value), function (x) paste(strwrap(x, ...), collapse = "\n"
))
}

label_wrapper10 <- function(variable, value) {
  label_wrapper(variable, value, 10)
}

d <- read.csv("avg_response_time.txt")
d$system = factor(d$system, levels = c('Vanilla PostgreSQL', 'Barista w/ Multi-Paxos', 'Barista w/ Standard Paxos'))

ggplot(d, aes(x=factor(system), y=response, fill=system)) + geom_bar(stat = "identity") + facet_grid(. ~ transaction, labeller= label_wrapper10) + ylab("Average Response Time (s)") + theme_bw() + theme(axis.text.x = element_blank()) + theme(axis.ticks.x = element_blank()) + theme(axis.title.x = element_blank()) + theme(panel.grid.major.x = element_blank()) + theme(plot.title = element_text(vjust = 1, face = "bold")) + labs(fill = "Systems") + labs(title = "Average Response Time")


ggsave(scale=1.5, file="avg_response_time.pdf", width=6.5, height=4)
dev.off()

d <- read.csv("90_response_time.txt")
d$system = factor(d$system, levels = c('Vanilla PostgreSQL', 'Barista w/ Multi-Paxos', 'Barista w/ Standard Paxos'))

ggplot(d, aes(x=factor(system), y=response, fill=system)) + geom_bar(stat = "identity") + facet_grid(. ~ transaction, labeller= label_wrapper10) + ylab("90% Response Time (s)") + theme_bw() + theme(axis.text.x = element_blank()) + theme(axis.ticks.x = element_blank()) + theme(axis.title.x = element_blank()) + theme(panel.grid.major.x = element_blank()) + theme(plot.title = element_text(vjust = 1, face = "bold")) + labs(fill = "Systems") + labs(title = "90% Response Time")


ggsave(scale=1.5, file="90_response_time.pdf", width=6.5, height=4)
dev.off()

d <- read.csv("throughput.txt")
d$system = factor(d$system, levels = c('Vanilla PostgreSQL', 'Barista w/ Multi-Paxos', 'Barista w/ Standard Paxos'))

ggplot(d, aes(x=factor(system), y=response, fill=system)) + geom_bar(stat = "identity") + facet_grid(. ~ transaction, labeller= label_wrapper10) + ylab("Total Transactions") + theme_bw() + theme(axis.text.x = element_blank()) + theme(axis.ticks.x = element_blank()) + theme(axis.title.x = element_blank()) + theme(panel.grid.major.x = element_blank()) + theme(plot.title = element_text(vjust = 1, face = "bold")) + labs(fill = "Systems") + labs(title = "Total Transactions in a Five Minute Window")


ggsave(scale=1.5, file="throughput.pdf", width=6.5, height=4)

dev.off()
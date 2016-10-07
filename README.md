# pixonictest
[![Build Status](https://travis-ci.org/antonkudinov/pixonictest.svg?branch=master)](https://travis-ci.org/antonkudinov/pixonictest)
[![Coverage Status](https://coveralls.io/repos/github/antonkudinov/pixonictest/badge.svg?branch=master)](https://coveralls.io/github/antonkudinov/pixonictest?branch=master)


# problem

На вход поступают пары (DateTime, Callable). Нужно реализовать систему, которая будет выполнять Callable для каждого пришедшего события в указанный DateTime, либо как можно скорее, в случае если система перегружена и не успевает выполнить все задания в запланированное для них время. Задачи должны выполняться в порядке согласно значению DateTime либо в порядке прихода события для равных DateTime. События могут приходить в произвольном порядке и добавление новых пар (DateTime, Callable) может вызываться из разных потоков.
Cистема перегружена если она в указанное в задании время не смогла завершить выполнение всех заданий назначенных на это время и уже наступила следующая минута. В этом случае надо выполнять именно эти задания сразу (в любое последующее время после запланированного).

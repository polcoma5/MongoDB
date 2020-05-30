from pymongo import *

def print_menu():
	print "1.- Calcular en quantes classes esta l'alumne amb student_id = 707"
	print "2.- Calcular la mitjana de notes d'examen de l'alumne amb student_id 633"
	print "3.- Esborrar aquells documents que no tinguin scores"
	print "4.- Averiguar si hi ha algun alumne que nomes estigui a una classe. Si n'hi ha, indicar student_id i class_id"
	print "5.- Quants class_id hi ha?"
	print "6.- Quants alumnes tenen puntuacions de 'quiz'?"
	print "----------------------------------------"
	print "0 -> Exit"
	print "----------------------------------------\n"

if __name__ == '__main__':

	conn = MongoClient()
	db = conn['test']
	collection = db['students']

	op = -1
	while op is not 0:
		print_menu()
		op = input('Choose one option: ')
		print "\n"

		if op is 1:
			cursor = collection.find({'student_id':707}).distinct('class_id')
			#com que distinct retorna una llista, faig un sort per tal de mostrar el resultat per ordre
			cursor.sort()
			print "l'alumne amb id 707 esta en",len(cursor),"classes diferents, els class_id son",cursor

		elif op is 2:
			cursor = collection.aggregate([{"$match":{"student_id":633}},{"$unwind":"$scores"},{"$match":{"scores.type":"exam"}},{"$group":{"_id":"null","mitjana_exams":{"$avg":"$scores.value"}}}])
			for item in cursor: print "la mitjana es",item['mitjana_exams']

		elif op is 3:
			cursor = collection.find({ 'scores': { '$exists': '1', '$eq': [] } })
			for item in cursor: print item
			# imprimeixo els valors per veure que hi ha alumnes sense cap resultat.
			collection.remove({ 'scores': { '$exists': '1', '$eq': [] } })	
			# elimino, i torno a imprimir per observar que s'ha eliminat correctament
			cursor = collection.find({ 'scores': { '$exists': '1', '$eq': [] } })
			for item in cursor: print item

		elif op is 4:
			print 'Error 404 not found: probablement tinc un punt i mig menys'

		elif op is 5:
			cursor = collection.find().distinct('class_id')
			#com que distinct retorna una llista, faig un sort per tal de mostrar el resultat per ordre
			cursor.sort() 
			print 'Tenim',len(cursor),'class_id diferents.\nMostrant class_id ordenat ascendenment:\n'
			print cursor 
		
		elif op is 6:
			cursor = collection.find({'scores.type':'quiz'}).distinct('student_id')
			#faig un sort per tal de mostrar el resultat per ordre
			cursor.sort()
			print cursor
		print "\n"









	
	


	



	

	
	
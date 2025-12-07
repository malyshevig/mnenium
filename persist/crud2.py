from flask import Flask, request
from flask_restful import Api, Resource, reqparse, abort, fields, marshal_with

app = Flask(__name__)
api = Api(app)





# Парсер аргументов для создания/обновления задач
task_parser = reqparse.RequestParser()
task_parser.add_argument('title', type=str, required=True, help='Title is required')
task_parser.add_argument('description', type=str, required=False)
task_parser.add_argument('completed', type=bool, required=False)

# Сериализация (маршалинг) полей
task_fields = {
    'id': fields.Integer,
    'title': fields.String,
    'description': fields.String,
    'completed': fields.Boolean
}




# Ресурс для работы с одной задачей
class Task(Resource):
    @marshal_with(task_fields)
    def get(self, task_id):
        """Получить задачу по ID"""
        return abort_if_task_not_found(task_id)

    @marshal_with(task_fields)
    def put(self, task_id):
        """Обновить задачу"""
        task = abort_if_task_not_found(task_id)
        args = task_parser.parse_args()

        task.title = args['title'] if args['title'] is not None else task.title
        task.description = args.get('description', task.description)
        task.completed = args.get('completed', task.completed)

        db.session.commit()
        return task

    def delete(self, task_id):
        """Удалить задачу"""
        task = abort_if_task_not_found(task_id)
        db.session.delete(task)
        db.session.commit()
        return {'message': f'Task {task_id} deleted'}


# Ресурс для работы со списком задач
class TaskList(Resource):
    @marshal_with(task_fields)
    def get(self):
        """Получить все задачи"""
        return TaskModel.query.all()

    @marshal_with(task_fields)
    def post(self):
        """Создать новую задачу"""
        args = task_parser.parse_args()

        # Проверка на существование задачи с таким же названием (опционально)
        existing_task = TaskModel.query.filter_by(title=args['title']).first()
        if existing_task:
            abort(409, message=f"Task with title '{args['title']}' already exists")

        task = TaskModel(
            title=args['title'],
            description=args.get('description'),
            completed=args.get('completed', False)
        )

        db.session.add(task)
        db.session.commit()
        return task, 201


# Ресурс для поиска задач
class TaskSearch(Resource):
    @marshal_with(task_fields)
    def get(self):
        """Поиск задач по параметрам"""
        title_filter = request.args.get('title')
        completed_filter = request.args.get('completed')

        query = TaskModel.query

        if title_filter:
            query = query.filter(TaskModel.title.contains(title_filter))
        if completed_filter:
            completed = completed_filter.lower() == 'true'
            query = query.filter_by(completed=completed)

        return query.all()


# Регистрация ресурсов
api.add_resource(TaskList, '/tasks')
api.add_resource(Task, '/tasks/<int:task_id>')
api.add_resource(TaskSearch, '/tasks/search')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
from flask import Flask, request, jsonify
from models import db, Employee, Department, Job
from flask_restful import Api, Resource
from sqlalchemy import create_engine, MetaData, Table
import pandas as pd
import psycopg2
from fastavro import writer, reader, parse_schema
from os import environ


connnection_string = environ.get('DB_URL')
output_dir = environ.get('OUTPUT_FILES')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = connnection_string

# Set up the database connection
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

# Initialize the API
api = Api(app)


# Create the database tables
with app.app_context():
    db.create_all()
    #db.session.add(Department('9999', 'Admin'))
    db.session.commit()


# Define a route to return a 'Hi' message
class Hi():
    @app.route('/hi', methods=['POST'])
    def hi():
        message = 'Hi'
        return jsonify(message)


# Define routes for creating, reading, updating, and deleting employees
class EmployeeResource(Resource):
    @app.route('/employees', methods=['POST'])
    def create_employee():
        """
        Create a new employee.

        Parameters:
        id (int): the employee's ID number
        name (str): the employee's name
        datetime (str): the employee's date of hire
        department_id (int): the ID number of the employee's department
        job_id (int): the ID number of the employee's job

        Returns:
        A JSON object containing the new employee's information.
        """
        data = request.get_json()
        new_employee = Employee(id=data['id'], name=data['name'], datetime=data['datetime'], department_id=data['department_id'], job_id=data['job_id'])
        try:
            db.session.add(new_employee)
            db.session.commit()
            return jsonify(new_employee.as_dict())
        except Exception as e:
            db.session.rollback()
            return jsonify({'error': str(e)})
        finally:
            db.session.close()

    @app.route('/employees/<int:id>', methods=['GET'])
    def get_employee(id):
        """
        Get an employee's information.

        Parameters:
        id (int): the employee's ID number

        Returns:
        A JSON object containing the employee's information.
        """
        try:
            employee = Employee.query.get(id)
            return jsonify(employee.as_dict())
        except Exception as e:
            return jsonify({'error': str(e)})
        finally:
            db.session.close()

    @app.route('/employees/<int:id>', methods=['PUT'])
    def update_employee(id):
        """
        Update an employee's information.

        Parameters:
        id (int): the employee's ID number
        name (str): the employee's name
        datetime (str): the employee's date of hire
        department_id (int): the ID number of the employee's department
        job_id (int): the ID number of the employee's job

        Returns:
        A JSON object containing the updated employee's information.
        """
        data = request.get_json()
        try:
            employee = Employee.query.get(id)
            employee.name = data['name']
            employee.datetime = data['datetime']
            employee.department_id = data['department_id']
            employee.job_id = data['job_id']
            db.session.commit()
            return jsonify(employee.as_dict())
        except Exception as e:
            db.session.rollback()
            return jsonify({'error': str(e)})
        finally:
            db.session.close()

    @app.route('/employees/<int:id>', methods=['DELETE'])
    def delete_employee(id):
        """
        Delete an employee.

        Parameters:
        id (int): the employee's ID number

        Returns:
        A message indicating that the employee was deleted.
        """
        try:
            employee = Employee.query.get(id)
            db.session.delete(employee)
            db.session.commit()
            return jsonify({'message': 'Employee deleted.'})
        except Exception as e:
            db.session.rollback()
            return jsonify({'error': str(e)})
        finally:
            db.session.close()

class DepartmentResource(Resource):
    """
    DepartmentResource is a Flask-RESTful Resource that defines the endpoints
    for creating, reading, updating, and deleting departments.
    """

    @app.route('/departments', methods=['POST'])
    def create_department():
        """
        Creates a new department.

        Parameters:
        - `id`: the department's ID
        - `department`: the department's name
        """
        data = request.get_json()
        new_department = Department(id=data['id'], department=data['department'])
        db.session.add(new_department)
        try:
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while creating the department.'}), 500
        return jsonify(new_department.as_dict())

    @app.route('/departments/<int:id>', methods=['GET'])
    def get_department(id):
        """
        Retrieves a department by id.

        Parameters:
        - `id`: the department's ID
        """
        department = Department.query.get(id)
        if department is None:
            return jsonify({'message': 'Department not found.'}), 404
        return jsonify(department.as_dict())

    @app.route('/departments/<int:id>', methods=['PUT'])
    def update_department(id):
        """
        Updates a department by id.

        Parameters:
        - `id`: the department's ID
        - `department`: the department's name
        """
        data = request.get_json()
        department = Department.query.get(id)
        if department is None:
            return jsonify({'message': 'Department not found.'}), 404
        department.department = data['department']
        try:
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while updating the department.'}), 500
        return jsonify(department.as_dict())

    @app.route('/departments/<int:id>', methods=['DELETE'])
    def delete_department(id):
        """
        Deletes a department by id.

        Parameters:
        - `id`: the department's ID
        """
        department = Department.query.get(id)
        if department is None:
            return jsonify({'message': 'Department not found.'}), 404
        try:
            db.session.delete(department)
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while deleting the department.'}), 500
        return jsonify(department.as_dict())

# Define endpoints for Job resource
class JobResource(Resource):
    @app.route('/jobs', methods=['POST'])
    def create_job():
        """
        Creates a new job in the database.

        :return: JSON object representing the new job
        """
        data = request.get_json()
        new_job = Job(id=data['id'], job=data['job'])
        db.session.add(new_job)
        try:
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while creating the job.'}), 500
        return jsonify(new_job.as_dict())
    
    @app.route('/jobs', methods=['GET'])
    def get_job(id):
        """
        Retrieves the job with the specified id from the database.

        :param id: id of the job to retrieve
        :return: JSON object representing the job
        """
        job = Job.query.get(id)
        if job is None:
            return jsonify({'message': 'Job not found.'}), 404
        return jsonify(job.as_dict())

    @app.route('/jobs', methods=['PUT'])
    def update_job(id):
        """
        Updates the job with the specified id in the database.

        :param id: id of the job to update
        :return: JSON object representing the updated job
        """
        data = request.get_json()
        job = Job.query.get(id)
        if job is None:
            return jsonify({'message': 'Job not found.'}), 404
        job.job = data['job']
        try:
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while updating the job.'}), 500
        return jsonify(job.as_dict())

    @app.route('/jobs', methods=['DELETE'])
    def delete_job(id):
        """
        Deletes the job with the specified id from the database.

        :param id: id of the job to delete
        :return: JSON object representing the deleted job
        """
        job = Job.query.get(id)
        if job is None:
            return jsonify({'message': 'Job not found.'}), 404
        try:
            db.session.delete(job)
            db.session.commit()
        except:
            db.session.rollback()
            return jsonify({'message': 'An error occurred while deleting the job.'}), 500
        return jsonify(job.as_dict())
    

class AvroFile(Resource):
    @app.route('/get_csv_db', methods=['GET'])
    def get_csv_db():

        # Connect to the database using SQLAlchemy
        engine = create_engine(connnection_string)

        # Load the data from the database into a Pandas dataframe
        df = pd.read_sql_table("department", engine)

        # Write the data to a CSV file
        df.to_csv('./job_table_data_db.csv', index=False)

        return {'message': 'CSV from db file created successfully'}


    def get_csv():

                # initialize list elements
        data = [10,20,30,40,50,60]
        
        # Create the pandas DataFrame with column name is provided explicitly
        df = pd.DataFrame(data, columns=['Numbers'])

        # Write the data to a CSV file
        df.to_csv('/home/output_files/job_table_data.csv', index=False)

        return {'message': 'CSV file created successfully'}
    
    @app.route('/avro', methods=['GET'])
    def avro():
        # Connect to the database using SQLAlchemy
        engine = create_engine(connnection_string)

        # Load the data from the database into a Pandas dataframe
        df = pd.read_sql_table("department", engine)

        # Define the Avro schema for the table
        schema = fastavro.schema.parse_schema({
            "type": "record",
            "name": "department",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "department", "type": "string"},
            ]
        })

        parsed_schema = parse_schema(schema)


        # 2. Convert pd.DataFrame to records - list of dictionaries
        records = df.to_dict('records')

        # 3. Write to Avro file
        with open(output_dir +'prices.avro', 'wb') as out:
            writer(out, parsed_schema, records)


        return {'message': 'Avro file created successfully'}


api.add_resource(AvroFile, '/avro')
api.add_resource(EmployeeResource, '/employees', '/employees/<int:id>')
api.add_resource(DepartmentResource, '/departments', '/departments/<int:id>')
api.add_resource(JobResource, '/jobs', '/jobs/<int:id>')


if __name__ == '__main__':
    app.run(debug=True)
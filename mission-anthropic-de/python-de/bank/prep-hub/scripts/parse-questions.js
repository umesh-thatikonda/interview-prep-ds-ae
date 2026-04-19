const fs = require('fs');
const path = require('path');

function parsePythonQuestions(content) {
  const questions = [];
  const sections = content.split('---').slice(1); // Skip header

  sections.forEach((section, index) => {
    const titleMatch = section.match(/## Q\d+: (.+)/);
    if (!titleMatch) return;

    const title = titleMatch[1].trim();
    const difficultyMatch = section.match(/\*\*Difficulty:\*\* (.+)/);
    const difficulty = difficultyMatch ? difficultyMatch[1].trim() : 'Easy';
    
    const problemStatementMatch = section.match(/### Problem Statement([\s\S]+?)###/);
    const statement = problemStatementMatch ? problemStatementMatch[1].trim() : '';

    const inputMatch = section.match(/### Input([\s\S]+?)###/);
    const inputCode = inputMatch ? inputMatch[1].match(/```python([\s\S]+?)```/) : null;
    const input = inputCode ? inputCode[1].trim() : '';

    const expectedMatch = section.match(/### Expected Output([\s\S]+?)###/);
    const expected = expectedMatch ? expectedMatch[1].trim() : '';

    questions.push({
      id: `py-${index + 1}`,
      title,
      difficulty,
      statement,
      type: 'python',
      initialCode: '# Write your solution here\n\n',
      testInput: input,
      expectedOutput: expected,
    });
  });

  return questions;
}

function parseSqlQuestions(content) {
  const questions = [];
  const sections = content.split('---').slice(1);

  sections.forEach((section, index) => {
    const titleMatch = section.match(/## Q\d+: (.+)/);
    if (!titleMatch) return;

    const title = titleMatch[1].trim();
    const difficultyMatch = section.match(/\*\*Difficulty:\*\* (.+)/);
    const difficulty = difficultyMatch ? difficultyMatch[1].trim() : 'Medium';

    const schemaMatch = section.match(/### Schema([\s\S]+?)###/);
    const schema = schemaMatch ? schemaMatch[1].trim() : '';

    const problemStatementMatch = section.match(/### Problem Statement([\s\S]+?)###/);
    const statement = problemStatementMatch ? problemStatementMatch[1].trim() : '';

    questions.push({
      id: `sql-${index + 1}`,
      title,
      difficulty,
      statement: `${statement}\n\n**Schema:**\n${schema}`,
      type: 'sql',
      initialCode: '-- Write your SQL query here\nSELECT * FROM ...',
      setupSql: '', // This would need to be populated with CREATE TABLE statements
    });
  });

  return questions;
}

const pythonPath = path.join(__dirname, '../../../../resources/golden_python_questions.md');
const sqlPath = path.join(__dirname, '../../../../resources/golden_sql_questions.md');

const pythonContent = fs.readFileSync(pythonPath, 'utf8');
const sqlContent = fs.readFileSync(sqlPath, 'utf8');

const pythonQuestions = parsePythonQuestions(pythonContent);
const sqlQuestions = parseSqlQuestions(sqlContent);

const allQuestions = [...pythonQuestions, ...sqlQuestions];

fs.writeFileSync(
  path.join(__dirname, '../data/questions.json'),
  JSON.stringify(allQuestions, null, 2)
);

console.log(`Successfully parsed ${pythonQuestions.length} Python and ${sqlQuestions.length} SQL questions.`);
